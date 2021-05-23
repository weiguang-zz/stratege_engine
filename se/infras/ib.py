from __future__ import annotations

import logging
import random
import threading
import time
from threading import Condition
from typing import *
from typing import Mapping

import trading_calendars
from ibapi import order_condition
from ibapi.client import EClient
from ibapi.commission_report import CommissionReport
from ibapi.common import BarData, HistoricalTickLast, ListOfHistoricalTickLast, OrderId, TickAttribLast, TickerId, \
    TickAttrib
from ibapi.contract import Contract, ContractDetails
from ibapi.execution import Execution, ExecutionFilter
from ibapi.order import Order as IBOrder
from ibapi.order_condition import OrderCondition, PriceCondition
from ibapi.tag_value import TagValue
from ibapi.ticktype import TickType, TickTypeEnum
from ibapi.wrapper import EWrapper
from pandas import Timedelta
from pandas import Timestamp
import math

from trading_calendars import TradingCalendar

from se.domain2.account.account import AbstractAccount, Order, OrderCallback, MKTOrder, OrderDirection, LimitOrder, \
    DelayMKTOrder, CrossMKTOrder, CrossDirection, Tick, OrderExecution, Operation, OrderStatus
from se.domain2.domain import send_email
from se.domain2.engine.engine import BidAsk, Scope
from se.domain2.monitor import alarm, AlarmLevel, EscapeParam, do_log, retry
from se.domain2.time_series.time_series import TimeSeriesFunction, Column, Asset, HistoryDataQueryCommand, \
    TSData, Price, Tick


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

    def tickPrice(self, reqId: TickerId, tickType: TickType, price: float, attrib: TickAttrib):
        super().tickPrice(reqId, tickType, price, attrib)
        if self.market_data_subscriber:
            self.market_data_subscriber.tickPrice(reqId, tickType, price, attrib)

    def tickSize(self, reqId: TickerId, tickType: TickType, size: int):
        super().tickSize(reqId, tickType, size)
        if self.market_data_subscriber:
            self.market_data_subscriber.tickSize(reqId, tickType, size)

    def tickString(self, reqId: TickerId, tickType: TickType, value: str):
        super().tickString(reqId, tickType, value)
        if self.market_data_subscriber:
            self.market_data_subscriber.tickString(reqId, tickType, value)

    def error(self, reqId: TickerId, errorCode: int, errorString: str):
        super().error(reqId, errorCode, errorString)

    def tickByTickAllLast(self, reqId: int, tickType: int, time: int, price: float, size: int,
                          tickAttribLast: TickAttribLast, exchange: str, specialConditions: str):
        try:
            super().tickByTickAllLast(reqId, tickType, time, price, size, tickAttribLast, exchange, specialConditions)
            if self.tick_subscriber:
                self.tick_subscriber.tickByTickAllLast(reqId, tickType, time, price, size, tickAttribLast,
                                                       exchange, specialConditions)
        except:
            import traceback
            logging.error("{}".format(traceback.format_exc()))

    def execDetails(self, reqId: int, contract: Contract, execution: Execution):
        try:
            super().execDetails(reqId, contract, execution)
            if self.account_subscriber:
                self.account_subscriber.execDetails(reqId, contract, execution)
        except:
            import traceback
            logging.error("{}".format(traceback.format_exc()))

    def commissionReport(self, commissionReport: CommissionReport):
        try:
            super().commissionReport(commissionReport)
            if self.account_subscriber:
                self.account_subscriber.commissionReport(commissionReport)
        except:
            import traceback
            logging.error("{}".format(traceback.format_exc()))

    def orderStatus(self, orderId: OrderId, status: str, filled: float, remaining: float, avgFillPrice: float,
                    permId: int, parentId: int, lastFillPrice: float, clientId: int, whyHeld: str, mktCapPrice: float):
        try:
            super().orderStatus(orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice,
                                clientId,
                                whyHeld, mktCapPrice)
            if self.account_subscriber:
                self.account_subscriber.orderStatus(orderId, status, filled, remaining, avgFillPrice, permId, parentId,
                                                    lastFillPrice, clientId, whyHeld, mktCapPrice)
        except:
            import traceback
            logging.error("{}".format(traceback.format_exc()))

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

    @alarm(target="尝试连接", freq=Timedelta(minutes=10))
    def try_connect(self):
        # 先清理掉无效的连接
        if self.cli.connState == EClient.CONNECTED:
            self.cli.disconnect()
        self.cli.connect(self.host, self.port, self.client_id)
        if self.cli.connState == EClient.CONNECTED and self.cli.reader.is_alive():
            threading.Thread(name="ib_msg_consumer", target=self.cli.run).start()
            # 等待客户端初始化成功
            time.sleep(3)
            # 重新订阅
            if self.tick_subscriber:
                if isinstance(self.tick_subscriber, IBTick):
                    self.tick_subscriber.re_connected()
            if self.market_data_subscriber:
                if isinstance(self.market_data_subscriber, IBMarketData):
                    self.market_data_subscriber.re_connected()
        else:
            raise RuntimeError("重新连接失败")

    def __init__(self, host, port, client_id):
        super().__init__()
        cli = EClient(self)
        self.cli = cli
        self.host = host
        self.port = port
        self.client_id = client_id
        self.account_subscriber = None
        self.tick_subscriber = None
        self.market_data_subscriber: EWrapper = None
        self._next_valid_id = None
        self.code_contract_map = {}
        self.try_connect()

        # 启动ping线程，如果与服务器的连接丢失，则会尝试重新连接
        def ping():
            # retry_count = 0
            while True:
                try:
                    if cli.connState != EClient.CONNECTED or not cli.reader.is_alive():
                        logging.info("尝试重新连接")
                        self.try_connect()
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
        if len(ss) > 4:
            contract.lastTradeDateOrContractMonth = ss[4]
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

    def sub(self, account_subscriber: EWrapper = None, tick_subscriber: EWrapper = None,
            market_data_subscriber: EWrapper = None):
        if account_subscriber:
            self.account_subscriber = account_subscriber
        if tick_subscriber:
            self.tick_subscriber = tick_subscriber
        if market_data_subscriber:
            self.market_data_subscriber = market_data_subscriber


class IBAccount(AbstractAccount, EWrapper):

    def valid_scope(self, scope: Scope):
        # 校验code是否能够查询到合约
        for code in scope.codes:
            contract = self.cli.code_to_contract(code)
            if not contract:
                raise RuntimeError("没有查询到合约，code:"+code)

    @do_log(target_name='取消订单', escape_params=[EscapeParam(index=0, key='self')])
    @alarm(target='取消订单', escape_params=[EscapeParam(index=0, key='self')])
    @retry(limit=3)
    def cancel_open_order(self, open_order: Order):
        if not open_order.ib_order_id:
            raise RuntimeError("ib_order_id is None")
        self.cli.cli.cancelOrder(orderId=open_order.ib_order_id)
        for i in range(4):
            time.sleep(0.5)
            if open_order.status == OrderStatus.CANCELED:
                break
        if open_order.status != OrderStatus.CANCELED:
            raise RuntimeError("取消订单失败")

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

        if (status == 'Submitted' or status == 'PreSubmitted') and order.status == OrderStatus.CREATED:
            order.status = OrderStatus.SUBMITTED
            if self.order_callback:
                self.order_callback.order_status_change(order, self)
        elif status == 'Inactive' and order.status != OrderStatus.FAILED:
            order.status = OrderStatus.FAILED
            if self.order_callback:
                self.order_callback.order_status_change(order, self)
        elif status == 'Cancelled' and order.status != OrderStatus.CANCELED:
            order.status = OrderStatus.CANCELED
            if self.order_callback:
                self.order_callback.order_status_change(order, self)

    @do_log(target_name='下单', escape_params=[EscapeParam(index=0, key='self')])
    @alarm(target='下单', escape_params=[EscapeParam(index=0, key='self')])
    @retry(limit=3)
    def place_order(self, order: Order):
        if order.ib_order_id:
            raise RuntimeError("非法的订单，订单已经被提交，请使用更新接口")
        self.orders.append(order)
        ib_order_id = self.cli.next_valid_id()
        ib_order = self.change_to_ib_order(order)
        self.ib_order_id_to_order[ib_order_id] = order
        self.cli.placeOrder(ib_order_id, self.cli.code_to_contract(order.code), ib_order)
        # 由于IB的下单操作也是异步的，订单的状态是异步推送过来的，无法确定这个延迟时间是多少
        # 大部分情况下，应该认为这个订单状态的推送是很小延迟的。但是不排除有些情况下这个延迟会比较长，所以使用分段sleep的方法进行检测
        # 如果2秒还未收到订单状态的变更的话，则认为是这个订单失败了，会重试
        for i in range(4):
            time.sleep(0.5)
            if order.status != OrderStatus.CREATED:
                break
        if order.status == OrderStatus.CREATED or order.status == OrderStatus.FAILED:
            if order.status == OrderStatus.CREATED:
                self.cancel_open_order(order)
            raise RuntimeError("place order error")
        order.ib_order_id = ib_order_id

    @do_log(target_name='更新订单', escape_params=[EscapeParam(index=0, key='self')])
    @alarm(target='更新订单', escape_params=[EscapeParam(index=0, key='self')])
    @retry(limit=3)
    def update_order(self, order: Order, reason: str):
        if not order.ib_order_id:
            raise RuntimeError("非法的订单，该订单还未提交")
        if not (order.status == OrderStatus.SUBMITTED or order.status == OrderStatus.PARTIAL_FILLED):
            raise RuntimeError("非法的订单状态")
        ib_order = self.change_to_ib_order(order)
        self.cli.placeOrder(order.ib_order_id, self.cli.code_to_contract(order.code), ib_order)
        order.update_reasons.append(reason)
        #  改为异步，因为更新订单通常只修改订单价格，所以几乎不会失败，改成异步的方式可以提高性能
        # # 等待1s，如果订单没有变为失败状态，则认为更新成功
        # time.sleep(1)
        # if order.status == OrderStatus.FAILED:
        #     raise RuntimeError("update order error")

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
                time.sleep(30 * 60)

        threading.Thread(name="account_save", target=save).start()

    def start_sync_order_executions_thread(self):
        # 启动订单同步线程，避免因为没有收到消息导致账户状态不一致
        def sync_order_executions():
            @alarm(level=AlarmLevel.ERROR, target="同步订单详情")
            def do_sync():
                if len(self.get_open_orders()) > 0:
                    logging.info("开始同步订单的执行详情")
                    req = Request.new_request()
                    exec_filter = ExecutionFilter()
                    exec_filter.clientId = self.cli.cli.clientId
                    self.cli.cli.reqExecutions(req.req_id, exec_filter)

            while True:
                try:
                    do_sync()
                except:
                    import traceback
                    err_msg = "同步订单详情失败:{}".format(traceback.format_exc())
                    logging.error(err_msg)
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
            ib_order.algoParams = [TagValue("adaptivePriority", 'Urgent')]
        elif isinstance(order, LimitOrder):
            ib_order.orderType = "LMT"
            ib_order.totalQuantity = order.quantity
            # 价格调整到两位小数
            ib_order.lmtPrice = round(order.limit_price, 2)
            ib_order.action = 'BUY' if order.direction == OrderDirection.BUY else 'SELL'
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

    def current_price(self, codes) -> Mapping[str, Price]:
        raise NotImplementedError

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

    def current_price(self, codes) -> Mapping[str, Price]:
        ret = {}
        for code in codes:
            if code in self.lastest_tick:
                tick = self.lastest_tick[code]
                ret[code] = Price(code, tick.price, tick.visible_time)
        return ret

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
        # self.start_check_realtime_data_thread()

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

    # def start_check_realtime_data_thread(self, time_threshold=Timedelta(minutes=30)):
    #     """
    #     如果有订阅的话，会启动实时数据监控线程，监控逻辑是：如果当前时间是盘前交易时间段，看AAPL最近的价格数据的时间，如果其
    #     价格数据的时间跟当前时间间隔超过阀值， 则认为获取实时数据是有问题的，发送监控告警
    #     :return:
    #     """
    #     us_calendar: TradingCalendar = trading_calendars.get_calendar("NYSE")
    #     pre_open_last = Timedelta(minutes=30, hours=5)
    #
    #     @alarm(level=AlarmLevel.ERROR, target='检查最新价格')
    #     def do_check():
    #         if self.has_sub_us_asset():
    #             now = Timestamp.now(tz='Asia/Shanghai')
    #             next_open = us_calendar.next_open(now)
    #             pre_open_start = next_open - pre_open_last
    #             if (pre_open_start + time_threshold) < now < next_open:
    #                 for code in self.sub_codes:
    #                     if code not in self.lastest_tick:
    #                         err_msg = "实时数据获取异常,没有{}的最新价格数据，当前时间:{}".format(code, now)
    #                         raise RuntimeError(err_msg)
    #                     else:
    #                         tick: Tick = self.lastest_tick[code]
    #                         if (now - tick.visible_time) > time_threshold:
    #                             err_msg = "实时数据获取异常,{}的最新价格数据为：{}，当前时间:{}". \
    #                                 format(code, tick.__dict__, now)
    #                             raise RuntimeError(err_msg)
    #                         else:
    #                             logging.info("实时数据正常，{}的最新价格数据为：{}，当前时间:{}".
    #                                          format(code, tick.__dict__, now))
    #
    #     def check_realtime_data():
    #         while True:
    #             try:
    #                 logging.info("开始检查实时数据")
    #                 do_check()
    #             except:
    #                 import traceback
    #                 err_msg = "检查实时数据异常:{}".format(traceback.format_exc())
    #                 logging.error(err_msg)
    #             time.sleep(10 * 60)
    #
    #     threading.Thread(name="check_realtime_data", target=check_realtime_data).start()

    # def has_sub_us_asset(self):
    #     for code in self.sub_codes:
    #         if "USD" in code:
    #             return True
    #     return False


class IBAdjustedDailyBar(TimeSeriesFunction):

    def current_price(self, codes) -> Mapping[str, Price]:
        raise NotImplementedError

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


class MarketData(TSData):
    def __init__(self, ts_type_name: str, visible_time: Timestamp, code: str, values: Dict[str, object]):
        super().__init__(ts_type_name, visible_time, code, values)


class IBMarketData(TimeSeriesFunction, EWrapper):

    def tickPrice(self, reqId: TickerId, tickType: TickType, price: float, attrib: TickAttrib):
        req = Request.find(reqId)
        code = req.code
        values = {
            'tick_type': tickType,
            'value': price,
            'attrib': attrib
        }
        if tickType == 1:
            # bid price
            if code in self.latest_bid_ask:
                self.latest_bid_ask[code].with_bid_price(price)
            else:
                bid_ask = BidAsk()
                bid_ask.with_bid_price(price)
                self.latest_bid_ask[code] = bid_ask
        elif tickType == 2:
            # ask price
            if code in self.latest_bid_ask:
                self.latest_bid_ask[code].with_ask_price(price)
            else:
                bid_ask = BidAsk()
                bid_ask.with_ask_price(price)
                self.latest_bid_ask[code] = bid_ask
        md = MarketData(self.name(), Timestamp.now(tz='Asia/Shanghai'), code=code, values=values)
        for sub in self.sub_map[code]:
            sub.on_data(md)

    def tickSize(self, reqId: TickerId, tickType: TickType, size: int):
        code = Request.find(reqId).code
        values = {
            'tick_type': tickType,
            'value': size,
        }
        if tickType == 0:
            # bid size
            if code in self.latest_bid_ask:
                self.latest_bid_ask[code].with_bid_size(size)
            else:
                bid_ask = BidAsk()
                bid_ask.with_bid_size(size)
                self.latest_bid_ask[code] = bid_ask
        elif tickType == 3:
            # ask price
            if code in self.latest_bid_ask:
                self.latest_bid_ask[code].with_ask_size(size)
            else:
                bid_ask = BidAsk()
                bid_ask.with_ask_size(size)
                self.latest_bid_ask[code] = bid_ask
        md = MarketData(self.name(), Timestamp.now(tz='Asia/Shanghai'), code=code, values=values)
        for sub in self.sub_map[code]:
            sub.on_data(md)

    def tickString(self, reqId: TickerId, tickType: TickType, value: str):
        code = Request.find(reqId).code
        if tickType == 48:
            # 45表示RTVolume
            values = value.split(';')
            if len(values[0]) > 0:
                price_time = Timestamp(int(values[2]), unit='ms', tz='Asia/Shanghai')
                p = Price(code, float(values[0]), price_time)
                self.latest_price[code] = p
        values = {
            'tick_type': tickType,
            'value': value,
        }
        md = MarketData(self.name(), Timestamp.now(tz='Asia/Shanghai'), code=code, values=values)
        for sub in self.sub_map[code]:
            sub.on_data(md)

    def name(self) -> str:
        return 'ibMarketData'

    def load_history_data(self, command: HistoryDataQueryCommand) -> List[TSData]:
        raise NotImplementedError

    def current_price(self, codes) -> Mapping[str, Price]:
        ret = {}
        for code in codes:
            if code in self.latest_price:
                ret[code] = self.latest_price[code]
        return ret

    def current_bid_ask(self, codes) -> Mapping[str, BidAsk]:
        ret = {}
        for code in codes:
            if code in self.latest_bid_ask:
                ret[code] = self.latest_bid_ask[code]
        return ret

    def load_assets(self) -> List[Asset]:
        raise NotImplementedError

    def do_sub(self, codes: List[str]):
        for code in codes:
            contract: Contract = self.client.code_to_contract(code)
            req = Request.new_request()
            self.client.cli.reqMktData(req.req_id, contract, '233', False, False, None)
            self.code_to_req[code] = req
            req.code = code

    def do_unsub(self, codes):
        for code in codes:
            req_id = self.code_to_req[code].req_id
            self.client.cli.cancelMktData(req_id)
            Request.clear(req_id)

    def __init__(self, host, port, client_id):
        super().__init__()

        cli = IBClient.find_client(host, port, client_id)
        if not cli:
            cli = IBClient(host, port, client_id)
            IBClient.registry(host, port, client_id, cli)
        cli.sub(market_data_subscriber=self)
        self.client = cli
        self.code_to_req: Mapping[str, Request] = {}
        self.latest_price: Mapping[str, Price] = {}
        self.latest_bid_ask: Mapping[str, BidAsk] = {}

    def columns(self) -> List[Column]:
        return []

    def re_connected(self):
        if len(self.sub_codes) > 0:
            logging.info("重新订阅实时数据，codes:{}".format(self.sub_codes))
            self.do_sub(self.sub_codes)
