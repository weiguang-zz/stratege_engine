from __future__ import annotations

import math
import random
import time
from threading import Condition

from ibapi.client import EClient
from ibapi.commission_report import CommissionReport
from ibapi.common import BarData, ListOfHistoricalTickLast, OrderId, TickerId, TickAttribLast, TickAttrib, \
    HistoricalTickLast
from ibapi.contract import Contract, ContractDetails
from ibapi.ticktype import TickType
from ibapi.wrapper import EWrapper
from pandas import DataFrame

from se2.domain.account import *
from se2.domain.time_series import HistoryDataQueryCommand, TSData, Column, Asset, RTTimeSeriesType, \
    BarHistoryTimeSeriesType, TSTypeRegistry

client: IBClient = None


def initialize(host: str, port: int, client_id: int):
    """
    初始化ib
    :param host:
    :param port:
    :param client_id:
    :return:
    """
    global client
    if client:
        raise RuntimeError("client已经被初始化了")
    client = IBClient(host, port, client_id)
    # 注册时序类型
    TSTypeRegistry.register(IBMinBar())
    TSTypeRegistry.register(IBCurrentPrice())
    TSTypeRegistry.register(IBAdjustedDailyBar())


class IBAccount(AbstractAccount):
    def match(self, data):
        raise NotImplementedError

    def do_place_order(self, order: Order):
        pass

    def do_cancel_order(self, order: Order):
        pass

    def do_update_order_price(self, order, new_price):
        pass

    def valid_scope(self, codes):
        pass


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


class ClientStatusCallback(metaclass=ABCMeta):
    @abstractmethod
    def re_connect(self):
        pass


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
            for callback in self.client_status_callbacks:
                callback.re_connect()
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
        self.client_status_callbacks: List[ClientStatusCallback] = []
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

    def register_client_status_callback(self, callback: ClientStatusCallback):
        if callback not in self.client_status_callbacks:
            self.client_status_callbacks.append(callback)


class IBCurrentPrice(RTTimeSeriesType, EWrapper, ClientStatusCallback):
    """
    IB实时数据
    """

    def re_connect(self):
        if len(self.sub_codes) > 0:
            logging.info("重新订阅实时数据，codes:{}".format(self.sub_codes))
            self.do_sub(self.sub_codes)

    def tickPrice(self, reqId: TickerId, tickType: TickType, price: float, attrib: TickAttrib):
        req = Request.find(reqId)
        code = req.code
        if tickType == 1:
            # bid price
            if code in self.current_price_map:
                self.current_price_map[code] = self.current_price_map[code].with_new_bid_price(price)
            else:
                cp: CurrentPrice = self._default_cp(code)
                cp = cp.with_new_bid_price(price)
                self.current_price_map[code] = cp
        elif tickType == 2:
            # ask price
            if code in self.current_price_map:
                self.current_price_map[code] = self.current_price_map[code].with_new_ask_price(price)
            else:
                self.current_price_map[code] = self._default_cp(code).with_new_ask_price(price)

        for sub in self.sub_map[code]:
            sub.on_data(self.current_price_map[code])

    def _default_cp(self, code):
        now = Timestamp.now(tz='Asia/Shanghai')
        return CurrentPrice(self.name(), now, code,
                            {"price": None, 'ask_price': None, 'ask_size': None, 'bid_price': None, 'bid_size': None})

    def tickSize(self, reqId: TickerId, tickType: TickType, size: int):
        code = Request.find(reqId).code
        values = {
            'tick_type': tickType,
            'value': size,
        }
        if tickType == 0:
            # bid size
            if code in self.current_price_map:
                self.current_price_map[code] = self.current_price_map[code].with_new_bid_size(size)
            else:
                self.current_price_map[code] = self._default_cp(code).with_new_bid_size(size)
        elif tickType == 3:
            # ask size
            if code in self.current_price_map:
                self.current_price_map[code] = self.current_price_map[code].with_new_ask_size(size)
            else:
                self.current_price_map[code] = self._default_cp(code).with_new_ask_size(size)
        for sub in self.sub_map[code]:
            sub.on_data(self.current_price_map[code])

    def tickString(self, reqId: TickerId, tickType: TickType, value: str):
        code = Request.find(reqId).code
        if tickType == 48:
            # 45表示RTVolume
            values = value.split(';')
            if len(values[0]) > 0:
                # 这个时间我们会丢弃，使用接收到数据的时间作为当前价格的时间戳
                # 因为收取到买卖价格的时候是没有时间戳的
                price_time = Timestamp(int(values[2]), unit='ms', tz='Asia/Shanghai')
                now = Timestamp.now(tz='Asia/Shanghai')
                try:
                    self._time_check(price_time, now)
                except:
                    pass
                new_price = float(values[0])
                if code in self.current_price_map:
                    self.current_price_map[code] = self.current_price_map[code].with_new_price(new_price)
                else:
                    self.current_price_map[code] = self._default_cp(code).with_new_price(new_price)

        for sub in self.sub_map[code]:
            sub.on_data(self.current_price_map[code])

    @alarm(level=AlarmLevel.ERROR, target="数据延迟检查", freq=Timedelta(minutes=1),
           escape_params=[EscapeParam(index=0, key='self')])
    def _time_check(self, server_time: Timestamp, receive_time: Timestamp):
        if (receive_time - server_time) > Timedelta(seconds=5):
            raise RuntimeError("接收的数据延迟过高")

    def name(self) -> str:
        return 'ibCurrentPrice'

    def current_price(self, codes) -> Mapping[str, CurrentPrice]:
        ret = {}
        for code in codes:
            if code in self.current_price_map:
                ret[code] = self.current_price_map[code]
        return ret

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

    def __init__(self):
        super().__init__()

        # cli = IBClient.find_client(host, port, client_id)
        # if not cli:
        #     cli = IBClient(host, port, client_id)
        #     IBClient.registry(host, port, client_id, cli)
        global client
        client.sub(market_data_subscriber=self)
        client.register_client_status_callback(self)
        self.client = client
        self.code_to_req: Mapping[str, Request] = {}
        self.current_price_map: Mapping[str, CurrentPrice] = {}


class IBAdjustedDailyBar(BarHistoryTimeSeriesType):

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

    def columns(self) -> List[Column]:
        columns = [Column("start_time", Timestamp, None, None, None), Column("open", float, None, None, None),
                   Column("high", float, None, None, None), Column("low", float, None, None, None),
                   Column("close", float, None, None, None), Column("volume", int, None, None, None)]
        return columns

    def __init__(self):
        super().__init__(current_price_change_start_offset=Timedelta(days=1),
                         current_price_change_end_offset=Timedelta(days=365 * 2))

        # cli = IBClient.find_client(host, port, client_id)
        # if not cli:
        #     cli = IBClient(host, port, client_id)
        #     IBClient.registry(host, port, client_id, cli)
        global client
        self.client = client
        self.cp_mem_cache: Mapping[str, DataFrame] = {}


class IBMinBar(BarHistoryTimeSeriesType):

    def __init__(self):
        super().__init__(current_price_change_start_offset=Timedelta(minutes=5),
                         current_price_change_end_offset=Timedelta(minutes=1440 * 10))
        # cli = IBClient.find_client(host, port, client_id)
        # if not cli:
        #     cli = IBClient(host, port, client_id)
        #     IBClient.registry(host, port, client_id, cli)
        global client
        self.client = client

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
