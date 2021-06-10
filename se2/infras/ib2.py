from __future__ import annotations

import math
import random
from threading import Condition

from ibapi.client import EClient
from ibapi.common import BarData, ListOfHistoricalTickLast, TickerId, TickAttribLast, TickAttrib, \
    ListOfHistoricalTick, ListOfHistoricalTickBidAsk, HistoricalTickBidAsk, HistoricalTick
from ibapi.contract import Contract, ContractDetails
from ibapi.ticktype import TickType
from ibapi.wrapper import EWrapper

from se2.domain.account import *
from se2.domain.time_series import HistoryDataQueryCommand, TSData, Column, Asset, RTTimeSeriesType, \
    BarHistoryTimeSeriesType, TSTypeRegistry, HistoryTimeSeriesType

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
    TSTypeRegistry.register(IBCurrentPrice())
    TSTypeRegistry.register(IBAdjustedDailyBar())
    TSTypeRegistry.register(IBMinBar())
    TSTypeRegistry.register(IBBidAsk())
    TSTypeRegistry.register(IBTrade())
    TSTypeRegistry.register(IB10SecondBar())


class ClientStatusCallback(metaclass=ABCMeta):
    @abstractmethod
    def re_connect(self):
        """
        client重新连接成功的回调
        :return:
        """
        pass


class Request(object):
    """
    对一次请求的建模，包含这次请求的id，请求对应的监听者，以及资产代号，资产代号只在获取资产价格数据的时候才是有的
    """
    id_to_request: Dict[int, Request] = {}

    def __init__(self, listener: EWrapper, code: str = None):
        self.condition: Condition = Condition()
        self.listener = listener
        self.id = self._random_id()
        self.code = code
        self.resp = None
        Request.id_to_request[self.id] = self

    def _random_id(self):
        while True:
            k = random.randint(0, 100000000)
            if k not in Request.id_to_request:
                return k

    @classmethod
    def clear(cls, req_id):
        return Request.id_to_request.pop(req_id)

    @classmethod
    def find(cls, req_id):
        return Request.id_to_request[req_id]


class IBClient(EWrapper, EClient):
    """
        封装了官方提供的client， 支持自动重连。
        所有的时序类型以及Account都依赖IBClient来获取数据，由于IBClient没有包含官方Client的所有接口，所以新增
        时序类型的时候，都需要扩展这个模型的方法。
        最理想实现应该是所有时序类型以及账户都能够直接使用官方的Client，并且将他们注册成为回调的目标。然而官方的Client实例只
        支持注册一个回调实例，所以需要某个回调实例来进行流量的分发，IBClient就承载了这样的职责。

        IBClient作为一个流量分发的组件，能够根据reqId将异步的响应数据分发给监听者，所有监听者都跟IBClient一样是一个EWrapper的实例，
        IBClient会分发到相应的监听者的相同方法上。 所以IBClient的所有回调方法的实现都是一样的，根据reqId找到监听者，然后调用响应监听者
        的回调方法。

        为了让IBClient具有所有官方的Client的方法，可以使用继承的方式来实现
        """

    def tickPrice(self, reqId: TickerId, tickType: TickType, price: float, attrib: TickAttrib):
        super().tickPrice(reqId, tickType, price, attrib)
        req = Request.find(reqId)
        req.listener.tickPrice(reqId, tickType, price, attrib)

    def tickSize(self, reqId: TickerId, tickType: TickType, size: int):
        super().tickSize(reqId, tickType, size)
        req = Request.find(reqId)
        req.listener.tickSize(reqId, tickType, size)

    def tickString(self, reqId: TickerId, tickType: TickType, value: str):
        super().tickString(reqId, tickType, value)
        req = Request.find(reqId)
        req.listener.tickString(reqId, tickType, value)

    def error(self, reqId: TickerId, errorCode: int, errorString: str):
        super().error(reqId, errorCode, errorString)
        logging.error("ib client error, req_id:{}, errorCode:{}, errorString:{}".format(reqId, errorCode, errorString))
        if reqId != -1:
            req = Request.find(reqId)
            req.listener.error(reqId, errorCode, errorString)

    def tickByTickAllLast(self, reqId: int, tickType: int, time: int, price: float, size: int,
                          tickAttribLast: TickAttribLast, exchange: str, specialConditions: str):
        super().tickByTickAllLast(reqId, tickType, time, price, size, tickAttribLast, exchange, specialConditions)
        req = Request.find(reqId)
        req.listener.tickByTickAllLast(reqId, tickType, time, price, size, tickAttribLast, exchange, specialConditions)

    def historicalData(self, reqId: int, bar: BarData):
        super().historicalData(reqId, bar)
        req = Request.find(reqId)
        req.listener.historicalData(reqId, bar)

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        super().historicalDataEnd(reqId, start, end)
        req = Request.find(reqId)
        req.listener.historicalDataEnd(reqId, start, end)

    def historicalTicksLast(self, reqId: int, ticks: ListOfHistoricalTickLast, done: bool):
        super().historicalTicksLast(reqId, ticks, done)
        req = Request.find(reqId)
        req.listener.historicalTicksLast(reqId, ticks, done)

    def historicalTicksBidAsk(self, reqId: int, ticks: ListOfHistoricalTickBidAsk, done: bool):
        super().historicalTicksBidAsk(reqId, ticks, done)
        req = Request.find(reqId)
        req.listener.historicalTicksBidAsk(reqId, ticks, done)

    def historicalTicks(self, reqId: int, ticks: ListOfHistoricalTick, done: bool):
        super().historicalTicks(reqId, ticks, done)
        req = Request.find(reqId)
        req.listener.historicalTicks(reqId, ticks, done)

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
    def try_connect(self, host, port, client_id):
        # 先清理掉无效的连接
        if self.connState == EClient.CONNECTED:
            self.disconnect()
        self.connect(host, port, client_id)
        if self.connState == EClient.CONNECTED and self.reader.is_alive():
            threading.Thread(name="ib_msg_consumer", target=self.run).start()
            # 等待客户端初始化成功
            time.sleep(3)
            # 通知监听者重新连接成功
            for callback in self.client_status_callbacks:
                callback.re_connect()
        else:
            raise RuntimeError("重新连接失败")

    def __init__(self, host, port, client_id):
        EWrapper.__init__(self)
        EClient.__init__(self, self)
        self._next_valid_id = None
        self.code_contract_map: Dict[str, Contract] = {}
        self.client_status_callbacks: List[ClientStatusCallback] = []
        self.try_connect(host, port, client_id)

        # 启动ping线程，如果与服务器的连接丢失，则会尝试重新连接
        def ping():
            # retry_count = 0
            while True:
                try:
                    if self.connState != EClient.CONNECTED or not self.reader.is_alive():
                        logging.info("尝试重新连接")
                        self.try_connect(host, port, client_id)
                except:
                    import traceback
                    logging.error("{}".format(traceback.format_exc()))

                time.sleep(10)

        threading.Thread(name="ib_ping", target=ping).start()
        logging.info("IBClient 初始化完成")

    def register_client_status_callback(self, callback: ClientStatusCallback):
        if callback not in self.client_status_callbacks:
            self.client_status_callbacks.append(callback)

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

    def query_contract(self, contract):
        req = Request(self)
        self.reqContractDetails(req.id, contract)

        if req.condition.acquire():
            req.condition.wait(20)
        if not req.resp:
            raise RuntimeError("没有获取到数据")
        resp = req.resp
        # 清理数据
        Request.clear(req.id)
        return resp


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
            req = Request(self, code)
            self.client.reqMktData(req.id, contract, '233', False, False, None)
            self.code_to_req[code] = req

    def do_unsub(self, codes):
        for code in codes:
            req_id = self.code_to_req[code].id
            self.client.cancelMktData(req_id)
            Request.clear(req_id)

    def __init__(self):
        super().__init__()
        global client
        client.register_client_status_callback(self)
        self.client = client
        self.code_to_req: Dict[str, Request] = {}
        self.current_price_map: Dict[str, CurrentPrice] = {}


class IBHistoryBar(EWrapper):

    def historicalData(self, reqId: int, bar: BarData):
        req = Request.find(reqId)
        if not req.resp:
            req.resp = [bar]
        else:
            req.resp.append(bar)

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        req = Request.find(reqId)
        if req.condition.acquire():
            req.condition.notifyAll()
            req.condition.release()

    def sync_load_data_in_batch(self, command: HistoryDataQueryCommand, bar_size_setting: str, what_to_show: str,
                                use_rth: int, format_date: int, keey_up_to_date: bool, char_options: list,
                                batch_size: int = 1440) -> Dict[str, List[BarData]]:
        """
        分批下载数据，下载区间支持使用HistoryDataQueryCommand来进行指定
        :param command:
        :param batch_size: 每批下载的数据时间范围，单位是分钟
        :return:
        """
        code_to_bars = {}
        for code in command.codes:
            bars: List[BarData] = []
            batch_end = command.end
            while True:
                batch_duration = "{} S".format(batch_size * 60)
                # 下面请求的返回结果不会包含end_date_time所对应的bar， IB使用bar的开始时间作为标签
                batch_bars = self.sync_load_history_bar(code, end_date_time=batch_end,
                                                        duration_str=batch_duration, bar_size_setting=bar_size_setting,
                                                        what_to_show=what_to_show, use_rth=use_rth,
                                                        format_date=format_date,
                                                        keep_up_to_date=keey_up_to_date, chart_options=char_options)
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

    def sync_load_history_bar(self, code, end_date_time: Timestamp, duration_str: str, bar_size_setting: str,
                              what_to_show: str, use_rth: int, format_date: int, keep_up_to_date: bool,
                              chart_options: List, timeout: int = 20):
        req = Request(self, code)
        contract = self.client.code_to_contract(code)
        self.client.reqHistoricalData(req.id, contract,
                                      end_date_time.strftime("%Y%m%d %H:%M:%S") if end_date_time else "",
                                      duration_str, bar_size_setting, what_to_show,
                                      use_rth, format_date, keep_up_to_date, chart_options)
        if req.condition.acquire():
            req.condition.wait(timeout)
        if not req.resp:
            self.client.cancelHistoricalData(req.id)
            raise RuntimeError("获取数据超时或者没有获取到数据")
        resp = req.resp
        # 清理数据
        Request.clear(req.id)
        # 返回排好序的数据
        return sorted(resp, key=lambda bar: bar.date)

    def __init__(self):
        super().__init__()
        global client
        self.client = client


class IBAdjustedDailyBar(BarHistoryTimeSeriesType, IBHistoryBar):

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
            bars: List[BarData] = self.sync_load_history_bar(code, None, "{} Y".format(ys), "1 day",
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
        BarHistoryTimeSeriesType.__init__(self, current_price_change_start_offset=Timedelta(days=1),
                                          current_price_change_end_offset=Timedelta(days=365 * 2))
        IBHistoryBar.__init__(self)


class IBMinBar(BarHistoryTimeSeriesType, IBHistoryBar):

    def __init__(self):
        BarHistoryTimeSeriesType.__init__(self, current_price_change_start_offset=Timedelta(minutes=5),
                                          current_price_change_end_offset=Timedelta(minutes=1440 * 10))
        IBHistoryBar.__init__(self)

    def name(self) -> str:
        return "ibMinBar"

    def load_history_data(self, command: HistoryDataQueryCommand) -> List[Bar]:
        code_to_bars: Dict[str, List[BarData]] = self.sync_load_data_in_batch(command, "1 min", "TRADES", 1,
                                                                              1, False, None)
        all_ts_datas: List[Bar] = []
        one_minute = Timedelta(minutes=1)
        for code in code_to_bars.keys():

            for bar in code_to_bars.get(code):
                dt = Timestamp(bar.date, tz='Asia/Shanghai')
                visible_time = dt + one_minute
                provider_data = {"start_time": dt, "open": bar.open, "high": bar.high, "low": bar.low, "close": bar.close,
                                 "volume": bar.volume}
                ts_data = Bar(self.name(), visible_time, code, self.parse(provider_data))
                all_ts_datas.append(ts_data)

        return all_ts_datas

    def load_assets(self) -> List[Asset]:
        pass

    def columns(self) -> List[Column]:
        columns = [Column("start_time", Timestamp, None, None, None), Column("open", float, None, None, None),
                   Column("high", float, None, None, None), Column("low", float, None, None, None),
                   Column("close", float, None, None, None), Column("volume", int, None, None, None)]
        return columns


class IB10SecondBar(BarHistoryTimeSeriesType, IBHistoryBar):

    def load_assets(self) -> List[Asset]:
        raise NotImplementedError

    def load_history_data(self, command: HistoryDataQueryCommand) -> List[Bar]:
        code_to_bars: Dict[str, List[BarData]] = self.sync_load_data_in_batch(command, bar_size_setting="10 secs",
                                                                              what_to_show="TRADES", use_rth=1,
                                                                              format_date=1, keey_up_to_date=False,
                                                                              char_options=[], batch_size=60)
        all_ts_datas: List[Bar] = []
        ten_second = Timedelta(seconds=10)
        for code in code_to_bars.keys():

            for bar in code_to_bars.get(code):
                dt = Timestamp(bar.date, tz='Asia/Shanghai')
                visible_time = dt + ten_second
                provider_data = {"start_time": dt, "open": bar.open, "high": bar.high, "low": bar.low, "close": bar.close,
                                 "volume": bar.volume}
                ts_data = Bar(self.name(), visible_time, code, provider_data)
                all_ts_datas.append(ts_data)

        return all_ts_datas


    def should_cache(self):
        return False

    def columns(self) -> List[Column]:
        return []

    def name(self) -> str:
        return 'ib10SecondBar'

    def __init__(self):
        BarHistoryTimeSeriesType.__init__(self, current_price_change_start_offset=Timedelta(seconds=10),
                                          current_price_change_end_offset=Timedelta(seconds=10 * 1000))
        IBHistoryBar.__init__(self)


class IBHistoryTick(EWrapper):

    def historicalTicksLast(self, reqId: int, ticks: ListOfHistoricalTickLast, done: bool):
        req = Request.find(reqId)
        if req.resp:
            req.resp.extend(ticks)
        else:
            req.resp = ticks
        if done:
            if req.condition.acquire():
                req.condition.notifyAll()
                req.condition.release()

    def historicalTicks(self, reqId: int, ticks: ListOfHistoricalTick, done: bool):
        req = Request.find(reqId)
        if req.resp:
            req.resp.extend(ticks)
        else:
            req.resp = ticks
        if done:
            if req.condition.acquire():
                req.condition.notifyAll()
                req.condition.release()

    def historicalTicksBidAsk(self, reqId: int, ticks: ListOfHistoricalTickBidAsk, done: bool):
        req = Request.find(reqId)
        if req.resp:
            req.resp.extend(ticks)
        else:
            req.resp = ticks
        if done:
            if req.condition.acquire():
                req.condition.notifyAll()
                req.condition.release()

    def sync_load_history_tick_in_batch(self, code, start_time: Timestamp, end_time: Timestamp,
                                        what_to_show: str, use_rth: int, ignore_size: bool, misc_options: List,
                                        timeout=20, batch_size=1000):
        if not start_time or not end_time:
            raise RuntimeError("start_time和end_time必传")
        res = []
        while True:
            ticks = self.sync_load_history_tick(code, start_time, None, batch_size, what_to_show, use_rth, ignore_size,
                                                misc_options, timeout)
            res.extend(ticks)
            batch_end_time = Timestamp(res[-1].time, unit='s', tz='Asia/Shanghai')
            next_batch_start = batch_end_time + Timedelta(seconds=1)
            if batch_end_time >= end_time:
                break
            if len(ticks) < batch_size:
                break
            start_time = next_batch_start
        return res

    def sync_load_history_tick(self, code, start_time: Timestamp, end_time: Timestamp, number_of_ticks: int,
                               what_to_show: str, use_rth: int, ignore_size: bool, misc_options: List, timeout=20):
        """
        同步的获取ticks数据，返回的数据根据时间排序好了
        :param code:
        :param start_time:
        :param end_time:
        :param number_of_ticks:
        :param what_to_show:
        :param use_rth:
        :param ignore_size:
        :param misc_options:
        :param timeout:
        :return:
        """
        req = Request(self, code)
        contract = self.client.code_to_contract(code)
        if not start_time and not end_time:
            raise RuntimeError("必须指定start_time或者end_time")
        if start_time and end_time:
            raise RuntimeError("只能指定start_time或者end_time")
        if number_of_ticks > 1000:
            raise RuntimeError("number_of_ticks不能大于1000")
        start_time_str = start_time.strftime("%Y%m%d %H:%M:%S") if start_time is not None else ""
        end_time_str = end_time.strftime("%Y%m%d %H:%M:%S") if end_time is not None else ""
        self.client.reqHistoricalTicks(req.id, contract, start_time_str, end_time_str, number_of_ticks, what_to_show,
                                       use_rth, ignore_size, misc_options)
        if req.condition.acquire():
            req.condition.wait(timeout)
        if not req.resp:
            raise RuntimeError("获取数据超时或者没有获取到数据")
        resp = req.resp
        Request.clear(req.id)
        # 返回排好序的数据
        return sorted(resp, key=lambda tick: tick.time)

    def __init__(self):
        super().__init__()
        global client
        self.client = client


class IBBidAsk(HistoryTimeSeriesType, IBHistoryTick):
    def current_price_in_history(self, codes, the_time: Timestamp, ts: TimeSeries) -> Mapping[str, CurrentPrice]:
        raise NotImplementedError

    def load_assets(self) -> List[Asset]:
        raise NotImplementedError

    def load_history_data(self, command: HistoryDataQueryCommand) -> List[TSData]:
        res = []
        for code in command.codes:
            bid_asks: List[HistoricalTickBidAsk] = \
                self.sync_load_history_tick_in_batch(code, command.start, command.end, "BID_ASK", 1,
                                                     True, None)
            for bid_ask in bid_asks:
                visible_time = Timestamp(bid_ask.time, unit='s', tz='Asia/Shanghai')
                provider_data = {"bid_price": bid_ask.priceBid, "ask_price": bid_ask.priceAsk,
                                 "bid_size": bid_ask.sizeBid, "ask_size": bid_ask.sizeAsk}
                ts_data = TSData(self.name(), visible_time, code, provider_data)
                res.append(ts_data)
        return res

    def columns(self) -> List[Column]:
        # 由于该时序类型不会缓存到本地，所以可以不用定义columns
        return []

    def name(self) -> str:
        return 'ibBidAsk'

    def should_cache(self):
        return False

    def __init__(self):
        HistoryTimeSeriesType.__init__(self)
        IBHistoryTick.__init__(self)


class IBTrade(HistoryTimeSeriesType, IBHistoryTick):
    def current_price_in_history(self, codes, the_time: Timestamp, ts: TimeSeries) -> Mapping[str, CurrentPrice]:
        raise NotImplementedError

    def load_assets(self) -> List[Asset]:
        raise NotImplementedError

    def load_history_data(self, command: HistoryDataQueryCommand) -> List[TSData]:
        res = []
        for code in command.codes:
            trades: List[HistoricalTick] = \
                self.sync_load_history_tick_in_batch(code, command.start, command.end, "TRADES", 1,
                                                     True, None)
            for trade in trades:
                visible_time = Timestamp(trade.time, unit='s', tz='Asia/Shanghai')
                provider_data = {"price": trade.price, "size": trade.size}
                ts_data = TSData(self.name(), visible_time, code, provider_data)
                res.append(ts_data)
        return res

    def columns(self) -> List[Column]:
        # 由于该时序类型不会缓存到本地，所以可以不用定义columns
        return []

    def name(self) -> str:
        return 'ibTrade'

    def should_cache(self):
        return False

    def __init__(self):
        HistoryTimeSeriesType.__init__(self)
        IBHistoryTick.__init__(self)
