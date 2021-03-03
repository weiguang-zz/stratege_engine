from __future__ import annotations

import logging
import random
import threading
import time
from threading import Condition
from typing import *

import trading_calendars
from ibapi import order_condition
from ibapi.client import EClient
from ibapi.commission_report import CommissionReport
from ibapi.common import BarData, HistoricalTickLast, ListOfHistoricalTickLast, OrderId, TickAttribLast, TickerId
from ibapi.contract import Contract, ContractDetails
from ibapi.execution import Execution, ExecutionFilter
from ibapi.order import Order as IBOrder
from ibapi.order_condition import OrderCondition, PriceCondition
from ibapi.tag_value import TagValue
from ibapi.wrapper import EWrapper
from pandas import Timedelta
from pandas import Timestamp
import math

from trading_calendars import TradingCalendar

from se.domain2.account.account import AbstractAccount, Order, OrderCallback, MKTOrder, OrderDirection, LimitOrder, \
    DelayMKTOrder, CrossMKTOrder, CrossDirection, Tick, OrderExecution, Operation, OrderStatus
from se.domain2.domain import send_email
from se.domain2.time_series.time_series import TimeSeriesFunction, Column, Asset, HistoryDataQueryCommand, \
    TSData, Price


class Request(object):
    id_to_request = {}

    def __init__(self):
        self.condition: Condition = Condition()
        self.req_id = self._random_id()
        self.resp = None
        Request.id_to_request[self.req_id] = self

    def _random_id(self):
        while True:
            k = random.randint(0, 100000000)
            if k not in Request.id_to_request:
                return k

    @classmethod
    def new_request(cls):
        return Request()

    @classmethod
    def clear(cls, req_id):
        return Request.id_to_request.pop(req_id)

    @classmethod
    def find(cls, reqId):
        return Request.id_to_request[reqId]


class IBClient(EWrapper):
    clients_map: Mapping[str, IBClient] = {}

    def error(self, reqId: TickerId, errorCode: int, errorString: str):
        super().error(reqId, errorCode, errorString)

    def tickByTickAllLast(self, reqId: int, tickType: int, time: int, price: float, size: int,
                          tickAttribLast: TickAttribLast, exchange: str, specialConditions: str):
        super().tickByTickAllLast(reqId, tickType, time, price, size, tickAttribLast, exchange, specialConditions)
        if self.tick_subscriber:
            self.tick_subscriber.tickByTickAllLast(reqId, tickType, time, price, size, tickAttribLast,
                                                   exchange, specialConditions)

    def execDetails(self, reqId: int, contract: Contract, execution: Execution):
        super().execDetails(reqId, contract, execution)
        if self.account_subscriber:
            self.account_subscriber.execDetails(reqId, contract, execution)

    def commissionReport(self, commissionReport: CommissionReport):
        super().commissionReport(commissionReport)
        if self.account_subscriber:
            self.account_subscriber.commissionReport(commissionReport)

    def orderStatus(self, orderId: OrderId, status: str, filled: float, remaining: float, avgFillPrice: float,
                    permId: int, parentId: int, lastFillPrice: float, clientId: int, whyHeld: str, mktCapPrice: float):
        super().orderStatus(orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId,
                            whyHeld, mktCapPrice)
        if self.account_subscriber:
            self.account_subscriber.orderStatus(orderId, status, filled, remaining, avgFillPrice, permId, parentId,
                                                lastFillPrice, clientId, whyHeld, mktCapPrice)

    def historicalData(self, reqId: int, bar: BarData):
        super().historicalData(reqId, bar)
        req = Request.find(reqId)
        if not req.resp:
            req.resp = [bar]
        else:
            req.resp.append(bar)

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        super().historicalDataEnd(reqId, start, end)
        req = Request.find(reqId)
        if req.condition.acquire():
            req.condition.notifyAll()
            req.condition.release()

    def historicalTicksLast(self, reqId: int, ticks: ListOfHistoricalTickLast, done: bool):
        super().historicalTicksLast(reqId, ticks, done)
        req = Request.find(reqId)
        if req.resp:
            req.resp.extend(ticks)
        else:
            req.resp = ticks
        if done:
            if req.condition.acquire():
                req.condition.notifyAll()
                req.condition.release()

    def contractDetails(self, reqId: int, contractDetails: ContractDetails):
        super().contractDetails(reqId, contractDetails)
        req = Request.find(reqId)
        if not req.resp:
            req.resp = [contractDetails.contract]
        else:
            req.resp.append(contractDetails.contract)

    def contractDetailsEnd(self, reqId: int):
        super().contractDetailsEnd(reqId)
        req = Request.find(reqId)
        if req.condition.acquire():
            req.condition.notifyAll()
            req.condition.release()

    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self._next_valid_id = orderId

    def __init__(self, host, port, client_id):
        super().__init__()
        cli = EClient(self)
        self.cli = cli
        self.account_subscriber = None
        self.tick_subscriber = None
        self._next_valid_id = None
        self.code_contract_map = {}

        def try_connect():
            # 先清理掉无效的连接
            if cli.connState == EClient.CONNECTED:
                cli.disconnect()
            cli.connect(host, port, client_id)
            if cli.connState == EClient.CONNECTED:
                threading.Thread(name="ib_msg_consumer", target=cli.run).start()
                # 等待客户端初始化成功
                time.sleep(3)

        try_connect()

        # 启动ping线程，如果与服务器的连接丢失，则会尝试重新连接
        def ping():
            retry_count = 0
            while True:
                try:
                    if cli.connState != EClient.CONNECTED or not cli.reader.is_alive():
                        retry_count += 1
                        if retry_count % 60 == 1:
                            # 每隔10分钟进行邮件提醒
                            logging.info("发送邮件通知")
                            send_email("【系统】连接断开，将会尝试重新连接",
                                       "client 信息: host:{}, port:{}, client_id:{}".
                                       format(self.cli.host, self.cli.port, self.cli.clientId))
                        logging.info("尝试重新连接")
                        try_connect()
                        if cli.connState == EClient.CONNECTED and cli.reader.is_alive():
                            retry_count = 0
                            logging.info("重新连接成功，发送邮件通知")
                            send_email("【系统】重新连接成功",
                                       "client 信息: host:{}, port:{}, client_id:{}".
                                       format(self.cli.host, self.cli.port, self.cli.clientId))
                            # 重新订阅
                            if self.tick_subscriber:
                                if isinstance(self.tick_subscriber, IBTick):
                                    self.tick_subscriber.re_connected()
                except:
                    import traceback
                    logging.error("{}".format(traceback.format_exc()))

                time.sleep(10)

        threading.Thread(name="ib_ping", target=ping).start()

    def req_history_data(self, code: str, end_date_time: Timestamp, duration_str, bar_size, what_to_show,
                         use_rth: int, format_date: int, keep_up_to_date, char_options) -> List[BarData]:
        req = Request.new_request()
        contract = self.code_to_contract(code)
        self.cli.reqHistoricalData(req.req_id, contract,
                                   end_date_time.strftime("%Y%m%d %H:%M:%S") if end_date_time else "",
                                   duration_str, bar_size,
                                   what_to_show, use_rth, format_date, keep_up_to_date, char_options)
        if req.condition.acquire():
            req.condition.wait(20)
        if not req.resp:
            self.cli.cancelHistoricalData(req.req_id)
            raise RuntimeError("获取数据超时或者没有获取到数据")
        resp = req.resp
        # 清理数据
        Request.clear(req.req_id)
        # 返回排好序的数据
        return sorted(resp, key=lambda bar: bar.date)

    def _req_history_ticks(self, code: str, start: Timestamp, end: Timestamp, nums: int, what_to_show: str,
                           use_rth: int,
                           ignore_size: bool, misc_options) -> List[HistoricalTickLast]:
        req = Request.new_request()
        contract = self.code_to_contract(code)
        self.cli.reqHistoricalTicks(req.req_id, contract,
                                    start.strftime("%Y%m%d %H:%M:%S") if start is not None else "",
                                    end.strftime("%Y%m%d %H:%M:%S"), nums, what_to_show,
                                    use_rth, ignore_size, misc_options)
        if req.condition.acquire():
            req.condition.wait(10)
        if not req.resp:
            raise RuntimeError("获取数据超时或者没有获取到数据")
        resp = req.resp
        Request.clear(req.req_id)
        return resp

    def req_min_bar(self, command: HistoryDataQueryCommand) -> Mapping[str, List[BarData]]:
        code_to_bars = {}
        for code in command.codes:
            bars: List[BarData] = []
            batch_end = command.end
            while True:
                batch_bars = self.req_history_data(code, end_date_time=batch_end, duration_str="86400 S",
                                                   bar_size='1 min',
                                                   what_to_show='TRADES', use_rth=1, format_date=1,
                                                   keep_up_to_date=False,
                                                   char_options=None)
                bars.extend(batch_bars[::-1])
                if command.start and command.end:
                    # 检查start时间
                    if Timestamp(bars[-1].date, tz='Asia/Shanghai') <= command.start:
                        break
                else:
                    # 检查window
                    if len(bars) >= command.window:
                        break
                batch_end = Timestamp(bars[-1].date, tz='Asia/Shanghai')
            code_to_bars[code] = bars
        return code_to_bars

    def req_tick(self, command: HistoryDataQueryCommand) -> Mapping[str, List[HistoricalTickLast]]:

        ticks_map = {}
        for code in command.codes:
            ticks: List[HistoricalTickLast] = []
            batch_end = command.end
            while True:
                # 如果指定了开始时间，则每次获取1000条，否则使用command里面定义的window
                window = 1000 if command.start else command.window
                batch_ticks = self._req_history_ticks(code, None, batch_end, nums=window,
                                                      what_to_show='TRADES',
                                                      use_rth=1, ignore_size=False, misc_options=None)
                ticks.extend(batch_ticks)
                if command.start and command.end:
                    if Timestamp(batch_ticks[0].time, unit='s', tz='Asia/Shanghai') <= command.start:
                        break
                else:
                    if len(ticks) >= command.window:
                        break
                batch_end = Timestamp(batch_ticks[0].time, unit='s', tz='Asia/Shanghai')
            ticks_map[code] = ticks

        return ticks_map

    def code_to_contract(self, code) -> Contract:
        if code in self.code_contract_map:
            return self.code_contract_map[code]
        contract = Contract()
        ss = code.split("_")
        contract.symbol = ss[0]
        contract.secType = ss[1]
        contract.currency = ss[2]
        contract.exchange = ss[3]
        contracts: List[Contract] = self.query_contract(contract)
        if len(contracts) != 1:
            raise RuntimeError("code不能唯一确定一个合约")
        self.code_contract_map[code] = contracts[0]
        return contracts[0]

    def contract_to_code(self, contract: Contract):
        return "_".join([contract.symbol, contract.secType, contract.currency, contract.exchange])

    def query_contract(self, contract):
        req = Request.new_request()
        self.cli.reqContractDetails(req.req_id, contract)

        if req.condition.acquire():
            req.condition.wait(20)
        if not req.resp:
            raise RuntimeError("没有获取到数据")
        resp = req.resp
        # 清理数据
        Request.clear(req.req_id)
        return resp

    def placeOrder(self, ib_order_id, contract, order):
        logging.info("下单, 合约:{}, 订单:{}".format(contract, order))
        self.cli.placeOrder(ib_order_id, contract, order)

    def next_valid_id(self):
        if not self._next_valid_id:
            raise RuntimeError("no next_valid_id")
        self._next_valid_id += 1
        return self._next_valid_id

    @classmethod
    def find_client(cls, host, port, client_id):
        key = "{}_{}_{}".format(host, port, client_id)
        if key in cls.clients_map:
            return cls.clients_map[key]
        return None

    @classmethod
    def registry(cls, host, port, client_id, cli: IBClient):
        key = "{}_{}_{}".format(host, port, client_id)
        cls.clients_map[key] = cli

    def sub(self, account_subscriber: EWrapper = None, tick_subscriber: EWrapper = None):
        if account_subscriber:
            self.account_subscriber = account_subscriber
        if tick_subscriber:
            self.tick_subscriber = tick_subscriber


class IBAccount(AbstractAccount, EWrapper):

    def execDetails(self, reqId: int, contract: Contract, execution: Execution):
        if execution.orderId not in self.ib_order_id_to_order:
            logging.info("该订单不是由该策略产生的订单，将会忽略, orderId:{}".format(execution.orderId))
            return
        if execution.execId not in self.execution_map:
            self.execution_map[execution.execId] = execution
            self._save_execution_if_needed(execution.execId)
        else:
            logging.info("重复的execution")

    def _save_execution_if_needed(self, exec_id: str):
        if exec_id in self.execution_map and exec_id in self.commission_map:
            execution = self.execution_map[exec_id]
            commission: CommissionReport = self.commission_map[exec_id]
            order = self.ib_order_id_to_order[execution.orderId]
            idx = exec_id.rindex('.')
            real_exec_id = exec_id[:idx]
            version = exec_id[idx + 1:]
            # 佣金等与佣金减去返佣
            the_commisson = commission.commission
            order_execution = OrderExecution(real_exec_id, int(version), the_commisson, execution.shares,
                                             execution.avgPrice,
                                             Timestamp(execution.time, tz='Asia/Shanghai'),
                                             Timestamp(execution.time, tz='Asia/Shanghai'), order.direction)
            self.order_filled(order, order_execution)

    def execDetailsEnd(self, reqId: int):
        pass

    def commissionReport(self, commissionReport: CommissionReport):
        if commissionReport.execId not in self.commission_map:
            self.commission_map[commissionReport.execId] = commissionReport
            self._save_execution_if_needed(commissionReport.execId)
        else:
            logging.info("重复的commission")

    def orderStatus(self, orderId: OrderId, status: str, filled: float, remaining: float, avgFillPrice: float,
                    permId: int, parentId: int, lastFillPrice: float, clientId: int, whyHeld: str, mktCapPrice: float):
        if orderId not in self.ib_order_id_to_order:
            logging.info("订单状态变更，但是该订单不是由该策略产生的订单，将会忽略, orderId:{}".format(orderId))
            return
        order: Order = self.ib_order_id_to_order[orderId]
        # 下面只需要处理下单失败的情况，比如因为合约不可卖空导致的失败，这种情况需要将订单状态置为FAILED
        if status == 'Inactive':
            order.status = OrderStatus.FAILED
            if self.order_callback:
                self.order_callback.order_status_change(order, self)


        # if orderId not in self.ib_order_id_to_order:
        #     logging.warning("该订单不是由该策略产生的订单，将会忽略, orderId:{}".format(orderId))
        #     return
        #
        # order: Order = self.ib_order_id_to_order[orderId]
        # if order.remaining() < remaining:
        #     raise RuntimeError("非法的成交")
        # real_filled = 0
        # if order.remaining() > remaining:
        #     real_filled = order.remaining() - remaining
        # if real_filled == 0:
        #     logging.warning("没有真的成交，将会忽略")
        #     return
        #
        # # 修改现金
        # net_value_before = self.cash + order.net_value()
        # order.order_filled_realtime(remaining, avgFillPrice)
        # new_cash = net_value_before - order.net_value()
        # self.cash = new_cash
        #
        # if order.direction == OrderDirection.SELL:
        #     real_filled = -real_filled
        #
        # if order.code not in self.positions:
        #     self.positions[order.code] = real_filled
        # else:
        #     self.positions[order.code] += real_filled
        # if self.positions[order.code] <= 0:
        #     self.positions.pop(order.code)
        #
        # if len(self.positions) <= 0:
        #     next_operation = self.current_operation.end(self.cash)
        #     self.history_operations.append(self.current_operation)
        #     self.current_operation = next_operation
        #
        # self.order_callback.order_status_change(order, self)
        # # 订单成交之后，保存当前账户状态
        # if remaining <= 0:
        #     self.save()

    def place_order(self, order: Order):
        self.orders.append(order)
        ib_order_id = self.cli.next_valid_id()
        order.ib_order_id = ib_order_id
        self.ib_order_id_to_order[ib_order_id] = order
        ib_order = self.change_to_ib_order(order)
        self.cli.placeOrder(ib_order_id, self.cli.code_to_contract(order.code), ib_order)

    def match(self, data):
        raise NotImplementedError

    def __init__(self, name: str, initial_cash: float):
        super().__init__(name, initial_cash)

        self.ib_order_id_to_order = {}
        self.execution_map: Mapping[str, Execution] = {}
        self.commission_map: Mapping[str, CommissionReport] = {}

    def start_save_thread(self):
        # 启动账户保存线程，每隔半小时会保存当前账户的操作数据
        def save():
            while True:
                try:
                    logging.info("开始保存账户数据")
                    self.save()
                except:
                    import traceback
                    err_msg = "保存账户失败:{}".format(traceback.format_exc())
                    logging.error(err_msg)
                    send_email("【系统】保存账户失败", err_msg)
                time.sleep(30 * 60)

        threading.Thread(name="account_save", target=save).start()

    def start_sync_order_executions_thread(self):
        # 启动订单同步线程，避免因为没有收到消息导致账户状态不一致
        def sync_order_executions():
            while True:
                try:
                    if len(self.get_open_orders()) > 0:
                        logging.info("开始同步订单的执行详情")
                        req = Request.new_request()
                        exec_filter = ExecutionFilter()
                        exec_filter.clientId = self.cli.cli.clientId
                        self.cli.cli.reqExecutions(req.req_id, exec_filter)
                except:
                    import traceback
                    err_msg = "同步订单详情失败:{}".format(traceback.format_exc())
                    logging.error(err_msg)
                    send_email("【系统】同步订单详情失败", err_msg)
                time.sleep(30)

        threading.Thread(name="sync_order_executions", target=sync_order_executions).start()

    def with_client(self, host, port, client_id):
        cli: IBClient = IBClient.find_client(host, port, client_id)
        if not cli:
            cli = IBClient(host, port, client_id)
            IBClient.registry(host, port, client_id, cli)
        cli.sub(self)
        self.cli = cli
        return self

    def change_to_ib_order(self, order: Order) -> IBOrder:
        # 市价单和限价单直接提交
        ib_order: IBOrder = IBOrder()
        if isinstance(order, MKTOrder):
            ib_order.orderType = "MKT"
            ib_order.totalQuantity = order.quantity
            ib_order.action = 'BUY' if order.direction == OrderDirection.BUY else 'SELL'
            # 设置自适应算法
            ib_order.algoStrategy = 'Adaptive'
            ib_order.algoParams = [TagValue("adaptivePriority", 'Normal')]
        elif isinstance(order, LimitOrder):
            ib_order.orderType = "LMT"
            ib_order.totalQuantity = order.quantity
            # 价格调整到两位小数
            ib_order.lmtPrice = round(order.limit_price, 2)
            ib_order.action = 'BUY' if order.direction == OrderDirection.BUY else 'SELL'
            ib_order.outsideRth = True
        else:
            # 穿越单和延迟单转化为IB的条件单
            if isinstance(order, DelayMKTOrder):
                ib_order.orderType = "MKT"
                ib_order.totalQuantity = order.quantity
                ib_order.action = 'BUY' if order.direction == OrderDirection.BUY else 'SELL'
                cond = order_condition.Create(OrderCondition.Time)
                cond.isMore = True
                time = (Timestamp.now(tz='Asia/Shanghai') + order.delay_time).strftime('%Y%m%d %H:%M:%S')
                cond.time(time)
                ib_order.conditions.append(cond)

            elif isinstance(order, CrossMKTOrder):
                ib_order.orderType = "MKT"
                ib_order.totalQuantity = order.quantity
                ib_order.action = 'BUY' if order.direction == OrderDirection.BUY else 'SELL'
                price_cond = order_condition.Create(OrderCondition.Price)
                contract = self.cli.code_to_contract(order.code)
                price_cond.conId = contract.conId
                price_cond.price = round(order.cross_price, 2)
                price_cond.isMore = True if order.cross_direction == CrossDirection.UP else False
                price_cond.exchange = contract.exchange
                price_cond.triggerMethod = PriceCondition.TriggerMethodEnum.Default
                ib_order.conditions.append(price_cond)

        return ib_order


class IBMinBar(TimeSeriesFunction):

    def do_sub(self, codes: List[str]):
        raise NotImplementedError

    def do_unsub(self, codes):
        raise NotImplementedError

    def __init__(self, host, port, client_id):
        super().__init__()

        cli = IBClient.find_client(host, port, client_id)
        if not cli:
            cli = IBClient(host, port, client_id)
            IBClient.registry(host, port, client_id, cli)
        self.client = cli

    def name(self) -> str:
        return "ibMinBar"

    def load_history_data(self, command: HistoryDataQueryCommand) -> List[TSData]:
        code_to_bars: Mapping[str, List[BarData]] = self.client.req_min_bar(command)
        all_ts_datas = []
        one_minute = Timedelta(minutes=1)
        for code in code_to_bars.keys():

            for bar in code_to_bars.get(code):
                dt = Timestamp(bar.date, tz='Asia/Shanghai')
                visible_time = dt + one_minute
                provider_data = {"date": dt, "open": bar.open, "high": bar.high, "low": bar.low, "close": bar.close,
                                 "volume": bar.volume}
                ts_data = TSData(self.name(), visible_time, code, self.parse(provider_data))
                all_ts_datas.append(ts_data)

        return all_ts_datas

    def load_assets(self) -> List[Asset]:
        pass

    def columns(self) -> List[Column]:
        columns = [Column("date", Timestamp, None, None, None), Column("open", float, None, None, None),
                   Column("high", float, None, None, None), Column("low", float, None, None, None),
                   Column("close", float, None, None, None), Column("volume", int, None, None, None)]
        return columns


class IBTick(TimeSeriesFunction, EWrapper):

    def tickByTickAllLast(self, reqId: int, tickType: int, time: int, price: float, size: int,
                          tickAttribLast: TickAttribLast, exchange: str, specialConditions: str):
        code = Request.find(reqId).code
        visible_time = Timestamp(time, unit='s', tz='Asia/Shanghai')
        self.lastest_tick[code] = Tick(self.name(), visible_time, code, price, size)
        if code not in self.sub_map or len(self.sub_map[code]) <= 0:
            logging.info("该tick数据没有订阅者, code:{}".format(code))
            return
        for sub in self.sub_map[code]:
            tick = Tick(self.name(), visible_time, code, price, size)
            sub.on_data(tick)

    def do_sub(self, codes: List[str]):
        for code in codes:
            contract: Contract = self.client.code_to_contract(code)
            req = Request.new_request()
            self.client.cli.reqTickByTickData(req.req_id, contract, "AllLast", 0, False)
            self.code_to_req[code] = req
            req.code = code

    def do_unsub(self, codes):
        for code in codes:
            req_id = self.code_to_req[code].req_id
            self.client.cli.cancelTickByTickData(req_id)
            Request.clear(req_id)

    def __init__(self, host, port, client_id):
        super().__init__()

        cli = IBClient.find_client(host, port, client_id)
        if not cli:
            cli = IBClient(host, port, client_id)
            IBClient.registry(host, port, client_id, cli)
        cli.sub(tick_subscriber=self)
        self.client = cli
        self.code_to_req: Mapping[str, Request] = {}
        self.lastest_tick: Mapping[str, Tick] = {}
        self.start_check_realtime_data_thread()

    def name(self) -> str:
        return "ibTick"

    def load_history_data(self, command: HistoryDataQueryCommand) -> List[TSData]:
        code_to_ticks: Mapping[str, List[HistoricalTickLast]] = self.client.req_tick(command)
        all_ts_datas = []
        for code in code_to_ticks.keys():

            for tick in code_to_ticks.get(code):
                visible_time = Timestamp(tick.time, unit='s', tz='Asia/Shanghai')

                provider_data = {"date": visible_time, "price": tick.price,
                                 "size": tick.size, "exchange": tick.exchange,
                                 "specialConditions": tick.specialConditions,
                                 "tickAttribs": str(tick.tickAttribLast)
                                 }
                ts_data = TSData(self.name(), visible_time, code, self.parse(provider_data))
                all_ts_datas.append(ts_data)

        return all_ts_datas

    def load_assets(self) -> List[Asset]:
        pass

    def columns(self) -> List[Column]:
        columns = [Column("date", Timestamp, None, None, None), Column("price", float, None, None, None),
                   Column("size", float, None, None, None), Column("exchange", str, None, None, None),
                   Column("specialConditions", str, None, None, None), Column("tickAttribs", str, None, None, None)]
        return columns

    def re_connected(self):
        if len(self.sub_codes) > 0:
            logging.info("重新订阅，codes:{}".format(self.sub_codes))
            self.do_sub(self.sub_codes)

    def start_check_realtime_data_thread(self, time_threshold=Timedelta(minutes=30)):
        """
        如果有订阅的话，会启动实时数据监控线程，监控逻辑是：如果当前时间是盘前交易时间段，看AAPL最近的价格数据的时间，如果其
        价格数据的时间跟当前时间间隔超过阀值， 则认为获取实时数据是有问题的，发送监控告警
        :return:
        """
        us_calendar: TradingCalendar = trading_calendars.get_calendar("NYSE")
        pre_open_last = Timedelta(minutes=30, hours=5)

        def check_realtime_data():
            while True:
                try:
                    # 如果没有订阅美股资产实时数据的话，则忽略
                    if self.has_sub_us_asset():
                        now = Timestamp.now(tz='Asia/Shanghai')
                        next_open = us_calendar.next_open(now)
                        pre_open_start = next_open - pre_open_last
                        if (pre_open_start + time_threshold) < now < next_open:
                            for code in self.sub_codes:
                                if code not in self.lastest_tick:
                                    err_msg = "实时数据获取异常,没有{}的最新价格数据，当前时间:{}".format(code, now)
                                    logging.error(err_msg)
                                    send_email("【系统】实时数据获取异常", err_msg)
                                else:
                                    tick: Tick = self.lastest_tick[code]
                                    if (now - tick.visible_time) > time_threshold:
                                        err_msg = "实时数据获取异常,{}的最新价格数据为：{}，当前时间:{}". \
                                            format(code, tick.__dict__, now)
                                        logging.error(err_msg)
                                        send_email("【系统】实时数据获取异常", err_msg)
                                    else:
                                        logging.info("实时数据正常，{}的最新价格数据为：{}，当前时间:{}".
                                                     format(code, tick.__dict__, now))
                        else:
                            logging.info("当前时间不是盘前交易时间段，不进行校验")
                    else:
                        logging.info("没有订阅美股数据，不需要监控实时数据")
                except:
                    import traceback
                    err_msg = "监控实时数据失败:{}".format(traceback.format_exc())
                    logging.error(err_msg)
                    send_email("【系统】监控实时数据失败", err_msg)
                time.sleep(10 * 60)

        threading.Thread(name="check_realtime_data", target=check_realtime_data).start()

    def has_sub_us_asset(self):
        for code in self.sub_codes:
            if "USD" in code:
                return True
        return False


class IBAdjustedDailyBar(TimeSeriesFunction):

    def name(self) -> str:
        return "ibAdjustedDailyBar"

    def should_cache(self):
        return False

    def load_history_data(self, command: HistoryDataQueryCommand) -> List[TSData]:
        if not command.calendar:
            raise RuntimeError("need calendar")
        start = command.start
        if not start:
            weeks = math.ceil(command.window / 5)
            start = command.end - Timedelta(weeks=weeks)

        ys = math.ceil((Timestamp.now(tz='Asia/Shanghai') - start).days / 365)
        total_ts_data: List[TSData] = []

        for code in command.codes:
            # 返回的bar的日期，是收盘时刻对应的UTC标准时间的日期部分
            bars: List[BarData] = self.client.req_history_data(code, None, "{} Y".format(ys), "1 day",
                                                               "ADJUSTED_LAST", 1, 1, False, None)
            for bar in bars:
                dt = Timestamp(bar.date, tz='UTC')
                visible_time = command.calendar.next_close(dt).tz_convert('Asia/Shanghai')
                start_time = (command.calendar.previous_open(visible_time) - Timedelta(minutes=1)).tz_convert(
                    'Asia/Shanghai')
                provider_data = {"start_time": start_time, "open": bar.open, "high": bar.high, "low": bar.low,
                                 "close": bar.close, "volume": bar.volume}
                ts_data = TSData(self.name(), visible_time, code, self.parse(provider_data))
                total_ts_data.append(ts_data)

        return total_ts_data

    def load_assets(self) -> List[Asset]:
        raise RuntimeError("not supported")

    def do_sub(self, codes: List[str]):
        raise NotImplementedError

    def do_unsub(self, codes):
        raise NotImplementedError

    def columns(self) -> List[Column]:
        columns = [Column("start_time", Timestamp, None, None, None), Column("open", float, None, None, None),
                   Column("high", float, None, None, None), Column("low", float, None, None, None),
                   Column("close", float, None, None, None), Column("volume", int, None, None, None)]
        return columns

    def __init__(self, host, port, client_id):
        super().__init__()

        cli = IBClient.find_client(host, port, client_id)
        if not cli:
            cli = IBClient(host, port, client_id)
            IBClient.registry(host, port, client_id, cli)
        self.client = cli
