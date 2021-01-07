import random
from threading import Condition
from typing import *

from ibapi.client import EClient
from ibapi.common import BarData, HistoricalTickLast, ListOfHistoricalTickLast
from ibapi.contract import Contract
from ibapi.wrapper import EWrapper
from pandas import Timestamp, DataFrame
from pandas._libs.tslibs.timedeltas import Timedelta

from se import config

from .time_series import TimeSeriesFunction, Column, Subscription, Asset, HistoryDataQueryCommand, TSData


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
        req.resp = ticks
        if req.condition.acquire():
            req.condition.notifyAll()
            req.condition.release()

    def __init__(self, host, port, client_id):
        super().__init__()
        cli = EClient(self)
        self.cli = cli
        self._next_valid_id = None
        cli.connect(host, port, client_id)
        import threading
        threading.Thread(name="ib_msg_consumer", target=cli.run).start()

    def _req_history_data(self, code: str, end_date_time: Timestamp, duration_str, bar_size, what_to_show,
                          use_rth: int, format_date: int, keep_up_to_date, char_options) -> List[BarData]:
        req = Request.new_request()
        contract = self.code_to_contract(code)
        self.cli.reqHistoricalData(req.req_id, contract, end_date_time.strftime("%Y%m%d %H:%M:%S"),
                                   duration_str, bar_size,
                                   what_to_show, use_rth, format_date, keep_up_to_date, char_options)
        if req.condition.acquire():
            req.condition.wait(10)
        if not req.resp:
            raise RuntimeError("没有获取到数据")
        resp = req.resp
        # 清理数据
        Request.clear(req.req_id)
        return resp

    def _req_history_ticks(self, code: str, start: Timestamp, end: Timestamp, nums: int, what_to_show: str,
                           use_rth: int,
                           ignore_size: bool, misc_options):
        req = Request.new_request()
        contract = self.code_to_contract(code)
        self.cli.reqHistoricalTicks(req.req_id, contract, start.strftime(""), end.strftime(""), nums, what_to_show,
                                    use_rth, ignore_size, misc_options)
        if req.condition.acquire():
            req.condition.wait(10)
        if not req.resp:
            raise RuntimeError("没有获取到数据")
        resp = req.resp
        Request.clear(req.req_id)
        return resp

    def req_min_bar(self, command: HistoryDataQueryCommand) -> Mapping[str, List[BarData]]:
        code_to_bars = {}
        for code in command.codes:
            bars: List[BarData] = []
            batch_end = command.end
            while True:
                batch_bars = self._req_history_data(code, end_date_time=batch_end, duration_str="86400 S",
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

    def req_tick(self, command: HistoryDataQueryCommand) -> List[HistoricalTickLast]:

        ticks: List[HistoricalTickLast] = []
        for code in command.codes:
            while True:
                batch_ticks = self._req_history_ticks(code, None, command.end, nums=1000, what_to_show='TRADES',
                                                      use_rth=1, ignore_size=False, misc_options=None)
                ticks.extend(batch_ticks)
                if command.start and command.end:
                    if Timestamp(ticks[-1].time) <= command.start:
                        break
                else:
                    if len(ticks) >= command.window:
                        break
        return ticks

    def code_to_contract(self, code) -> Contract:
        contract = Contract()
        ss = code.split("_")
        contract.symbol = ss[0]
        contract.secType = ss[1]
        contract.currency = ss[2]
        contract.exchange = ss[3]
        return contract

    def contract_to_code(self, contract: Contract):
        return "_".join([contract.symbol, contract.secType, contract.currency, contract.exchange])


client = IBClient(config.get("ib", "host"), config.getint("ib", 'port'), config.getint("ib", "client_id"))


class IBMinBar(TimeSeriesFunction):
    def name(self) -> str:
        return "ibMinBar"

    def load_history_data(self, command: HistoryDataQueryCommand) -> List[TSData]:
        code_to_bars: Mapping[str, List[BarData]] = client.req_min_bar(command)
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

    def sub_func(self, subscription: Subscription):
        pass

    def unsub_func(self, subscription: Subscription):
        pass

    def columns(self) -> List[Column]:
        columns = [Column("date", Timestamp, None, None, None), Column("open", float, None, None, None),
                   Column("high", float, None, None, None), Column("low", float, None, None, None),
                   Column("close", float, None, None, None), Column("volume", int, None, None, None)]
        return columns

