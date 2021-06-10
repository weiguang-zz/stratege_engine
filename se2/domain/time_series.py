# 该模块是为了解决时序数据定义、存储和查询问题
from __future__ import annotations
import json
import logging
from abc import ABCMeta, abstractmethod
from typing import Dict, List, Type, Mapping

import trading_calendars
from pandas import Timestamp, DataFrame, Timedelta, Series
from trading_calendars import TradingCalendar

from se2.domain.common import BeanContainer


class TSData(object):
    def __init__(self, ts_type_name: str, visible_time: Timestamp, code: str, values: Dict[str, object]):
        self.ts_type_name = ts_type_name
        self.visible_time = visible_time
        self.code = code
        self.values = values

    def to_dict(self):
        dt = {}
        dt.update(self.values)
        dt['visible_time'] = self.visible_time
        dt['code'] = self.code
        return dt


class Bar(TSData):

    def __init__(self, ts_type_name: str, visible_time: Timestamp, code: str, values: Dict[str, object]):
        super().__init__(ts_type_name, visible_time, code, values)
        # 检查values
        for column_name in ['open', 'high', 'low', 'close', 'volume', 'start_time']:
            if column_name not in values:
                raise RuntimeError("非法的bar")

    @property
    def open(self):
        return self.values['open']

    @property
    def high(self):
        return self.values['high']

    @property
    def low(self):
        return self.values['low']

    @property
    def close(self):
        return self.values['close']

    @property
    def start_time(self):
        return self.values['start_time']


class Tick(TSData):
    def __init__(self, ts_type_name: str, visible_time: Timestamp, code: str, values: Dict[str, object]):
        super().__init__(ts_type_name, visible_time, code, {})
        for column_name in ['price', 'size']:
            if column_name not in values:
                raise RuntimeError("非法的tick")

    @property
    def price(self):
        return self.values['price']

    @property
    def size(self):
        return self.values['size']


class HistoryDataQueryCommand(object):
    """
    客户端的请求
    """

    def __init__(self, start: Timestamp, end: Timestamp, codes: List[str], window: int = 100):
        if not end:
            end = Timestamp.now(tz='Asia/Shanghai')
        if not (start or window > 0):
            raise RuntimeError("必须指定start和end 或者 end和window")
        if not codes or len(codes) <= 0:
            raise RuntimeError("codes不能为空")
        self.start = start
        self.end = end
        self.codes = codes
        self.window = window
        self.calendar: TradingCalendar = trading_calendars.get_calendar('NYSE')

    def with_calendar(self, trading_calendar: TradingCalendar):
        self.calendar = trading_calendar

    def to_single_code_command(self):
        commands: List[SingleCodeQueryCommand] = []
        for code in self.codes:
            commands.append(SingleCodeQueryCommand(self.start, self.end, code, self.window))
        return commands


class SingleCodeQueryCommand(HistoryDataQueryCommand):
    """
    一次下载的请求
    """

    def __init__(self, start: Timestamp, end: Timestamp, code: str, window: int = 100):
        super().__init__(start, end, [code], window)
        self.start = start
        self.end = end
        self.code = code
        self.window = window

    def minus(self, data_record: DataRecord) -> List:
        """
        要下载的数据减去已经存在的数据，就是增量的要下载的数据。
        为了降低复杂度， 保证下载数据之后，数据库的数据要连续
        :param data_record:
        :return:
        """
        small_start = self.start if self.start < data_record.start else data_record.start
        big_end = self.end if self.end > data_record.end else data_record.end
        increment_commands = []
        if small_start < data_record.start:
            increment_commands.append(SingleCodeQueryCommand(small_start, data_record.start, self.code))
        if big_end > data_record.end:
            increment_commands.append(SingleCodeQueryCommand(data_record.end, big_end, self.code))
        return increment_commands

        pass


class DataRecord(object):
    """
    本地缓存了的数据
    """

    def __init__(self, code: str, start: Timestamp, end: Timestamp):
        self.code = code
        self.start = start.tz_convert("Asia/Shanghai")
        self.end = end.tz_convert("Asia/Shanghai")

    def update(self, command: SingleCodeQueryCommand):
        if not isinstance(command, SingleCodeQueryCommand):
            raise RuntimeError("wrong type")
        if command.code != self.code:
            raise RuntimeError("wrong code")
        if command.start < self.start:
            self.start = command.start
        if command.end > self.end:
            self.end = command.end


class Column(object):
    """
    一个时序数据的values应该包含哪些列，这些列的定义由这个模型来定义，它包含列的名称、类型、解析函数、序列化和反序列化函数
    """

    def __init__(self, name: str, tp: Type, parse_func, serialize_func, deserialize_func):
        self.name = name
        self.tp = tp
        self.parse_func = parse_func
        self.serialize_func = serialize_func
        self.deserialize_func = deserialize_func

    def parse(self, provider_value: object):
        if self.parse_func:
            return self.parse_func(provider_value)
        else:
            if type(provider_value) == self.tp:
                return provider_value
            elif type(provider_value) in [str, int, float]:
                return self.tp(provider_value)
            else:
                raise RuntimeError("无法解析")

    def serialize(self, value: object):
        if type(value) != self.tp:
            raise RuntimeError("value类型不对")
        if self.tp in [str, int, float]:
            return value
        if self.serialize_func:
            return self.serialize_func(value)
        if isinstance(value, Timestamp):
            return str(value)

        raise RuntimeError("无法序列化")

    def deserialize(self, serialized_value: object):
        if type(serialized_value) == self.tp:
            return serialized_value
        if self.deserialize_func:
            return self.deserialize_func(serialized_value)
        if self.tp == Timestamp:
            return Timestamp(serialized_value)
        raise RuntimeError("无法反序列化")


class TimeSeriesSubscriber(metaclass=ABCMeta):
    @abstractmethod
    def on_data(self, data: TSData):
        pass


class CurrentPrice(TSData):
    """
    对实时价格的建模，该模型在回测和实时环境中都会用到，回测环境中不要使用with开头的方法，with方法会返回根据当前时间生成一个新的CurrentPrice
    实例，仅适用于数据连续变化的场景
    """

    def __init__(self, ts_type_name: str, visible_time: Timestamp, code: str, values: Dict[str, object]):
        super().__init__(ts_type_name, visible_time, code, values)
        for name in ['price', 'ask_price', 'ask_size', 'bid_price', 'bid_size']:
            if name not in values:
                raise RuntimeError("非法的current price")

    def with_new_ask_price(self, ask_price):
        now = Timestamp.now(tz='Asia/Shanghai')
        new_values: Dict = self.values.copy()
        new_values.update({"ask_price": ask_price})
        new_cp: CurrentPrice = CurrentPrice(self.ts_type_name, now, self.code, new_values)
        return new_cp

    def with_new_ask_size(self, ask_size):
        now = Timestamp.now(tz='Asia/Shanghai')
        new_values: Dict = self.values.copy()
        new_values.update({"ask_size": ask_size})
        new_cp: CurrentPrice = CurrentPrice(self.ts_type_name, now, self.code, new_values)
        return new_cp

    def with_new_bid_price(self, bid_price):
        now = Timestamp.now(tz='Asia/Shanghai')
        new_values: Dict = self.values.copy()
        new_values.update({"bid_price": bid_price})
        new_cp: CurrentPrice = CurrentPrice(self.ts_type_name, now, self.code, new_values)
        return new_cp

    def with_new_bid_size(self, bid_size):
        now = Timestamp.now(tz='Asia/Shanghai')
        new_values: Dict = self.values.copy()
        new_values.update({"bid_size": bid_size})
        new_cp: CurrentPrice = CurrentPrice(self.ts_type_name, now, self.code, new_values)
        return new_cp

    def with_new_price(self, price):
        now = Timestamp.now(tz='Asia/Shanghai')
        new_values: Dict = self.values.copy()
        new_values.update({"price": price})
        new_cp: CurrentPrice = CurrentPrice(self.ts_type_name, now, self.code, new_values)
        return new_cp

    @property
    def price(self):
        return self.values['price']

    @property
    def ask_price(self):
        return self.values['ask_price']

    @property
    def ask_size(self):
        return self.values['ask_size']

    @property
    def bid_price(self):
        return self.values['bid_price']

    @property
    def bid_size(self):
        return self.values['bid_size']


class Asset(object):
    pass


class TimeSeriesType(metaclass=ABCMeta):
    """
    每一个具体的时间序列都应该有一个具体的实现，来定义如何下载数据、如何获取当前的资产价格、如何订阅和反订阅数据以及这个时间序列由哪些列组成。
    """

    @abstractmethod
    def name(self) -> str:
        pass


class HistoryTimeSeriesType(TimeSeriesType, metaclass=ABCMeta):

    @abstractmethod
    def current_price_in_history(self, codes, the_time: Timestamp, ts: TimeSeries) -> Mapping[str, CurrentPrice]:
        """
        在回测环境中获取实时数据
        :param codes:
        :param the_time:
        :param ts:
        :return:
        """
        pass

    @abstractmethod
    def load_assets(self) -> List[Asset]:
        pass

    def should_cache(self):
        return True

    @abstractmethod
    def load_history_data(self, command: HistoryDataQueryCommand) -> List[TSData]:
        pass

    @abstractmethod
    def columns(self) -> List[Column]:
        pass

    def __init__(self):
        cols = self.columns()
        col_map = {}
        for col in cols:
            col_map[col.name] = col
        self.column_map = col_map

    def parse(self, provider_data: Mapping[str, object]):
        parsed_value = {}
        for key in provider_data.keys():
            pv = provider_data[key]
            column = self.column_map[key]
            if column:
                parsed_value[key] = column.parse(pv)
        return parsed_value

    def serialize(self, values: Mapping):
        """
        将map类型的values类型序列化成str
        :param values:
        :return:
        """
        dt: Dict[str, object] = {}
        for col_name in values.keys():
            column: Column = self.column_map[col_name]
            dt[col_name] = column.serialize(values[col_name])
        return json.dumps(dt)

    def deserialized(self, values_str: str):
        """
        将str类型反序列化map类型
        :param values_str:
        :return:
        """
        values = {}
        dt: dict = json.loads(values_str)
        for col_name in dt:
            column: Column = self.column_map[col_name]
            values[col_name] = column.deserialize(dt[col_name])

        return values


class BarHistoryTimeSeriesType(HistoryTimeSeriesType, metaclass=ABCMeta):

    def current_price_in_history(self, codes, the_time: Timestamp, ts: TimeSeries) -> Mapping[str, CurrentPrice]:
        """
        获取历史上某个时间点的价格，因为该时序类型是Bar类型，所以只能获取交易日开盘以及收盘的价格。 为了提高性能，该方法会使用内存缓存
        :param codes:
        :param the_time:
        :param ts:
        :return:
        """
        ret = {}
        for code in codes:
            if code not in self.cp_mem_cache:
                self._rebuild_cp_cache(code, the_time, ts)
            else:
                price_df: DataFrame = self.cp_mem_cache[code]
                if len(price_df.loc[the_time:]) < 1:
                    self._rebuild_cp_cache(code, the_time, ts)

            price_df: DataFrame = self.cp_mem_cache[code]
            # build cache的时候会向前查询一段时间，如果以visible_time作为索引查询截止到某个时间的数据，返回为空的话，则这个时间为开盘时间
            pre_df: DataFrame = price_df.loc[:the_time]
            if len(pre_df) > 0 and (pre_df.iloc[-1].name[0] == the_time):
                price = pre_df.iloc[-1]['close']
                cp = CurrentPrice(self.name(), the_time, code,
                                  {'price': price, 'ask_price': None, 'ask_size': None, 'bid_price': None,
                                   'bid_size': None})
            else:
                ser: Series = price_df.loc[the_time:].iloc[0]
                if ser['start_time'] == the_time:
                    price = ser['open']
                    cp = CurrentPrice(self.name(), the_time, code,
                                      {'price': price, 'ask_price': None, 'ask_size': None, 'bid_price': None,
                                       'bid_size': None})
                else:
                    raise RuntimeError("无法获取当前价格，时间:{}, code:{}".format(str(the_time), code))

            ret[code] = cp
        return ret

    def _rebuild_cp_cache(self, code: str, the_time: Timestamp, ts: TimeSeries):
        start = the_time - self.current_price_change_start_offset
        end = the_time + self.current_price_change_end_offset
        command = HistoryDataQueryCommand(start, end, [code])
        df: DataFrame = ts.history_data(command, from_local=True)
        self.cp_mem_cache[code] = df

    def __init__(self, current_price_change_start_offset: Timedelta, current_price_change_end_offset: Timedelta):
        super().__init__()
        self.cp_mem_cache: Dict[str, DataFrame] = {}
        self.current_price_change_start_offset = current_price_change_start_offset
        self.current_price_change_end_offset = current_price_change_end_offset


class RTTimeSeriesType(TimeSeriesType, metaclass=ABCMeta):
    @abstractmethod
    def current_price(self, codes) -> Dict[str, CurrentPrice]:
        """
        获取实时数据
        :param codes:
        :return:
        """
        pass

    def sub_func(self, subscriber: TimeSeriesSubscriber, codes: List[str]):
        need_sub_codes = [code for code in codes if code not in self.sub_codes]
        if len(need_sub_codes) > 0:
            self.do_sub(need_sub_codes)
            self.sub_codes.extend(need_sub_codes)
        if subscriber:
            for code in codes:
                if code in self.sub_map:
                    if subscriber not in self.sub_map[code]:
                        self.sub_map[code].append(subscriber)
                else:
                    self.sub_map[code] = [subscriber]
        else:
            for code in codes:
                if code not in self.sub_map:
                    self.sub_map[code] = []


    @abstractmethod
    def do_sub(self, codes: List[str]):
        pass

    def unsub_func(self, subscriber: TimeSeriesSubscriber, codes: List[str]):
        need_unsub_codes = [code for code in codes if code in self.sub_codes]
        if len(need_unsub_codes) > 0:
            self.do_unsub(need_unsub_codes)
            for code in need_unsub_codes:
                self.sub_codes.remove(code)
        for code in codes:
            if subscriber and code in self.sub_map and subscriber in self.sub_map[code]:
                self.sub_map[code].remove(subscriber)

    @abstractmethod
    def do_unsub(self, codes):
        pass

    def __init__(self):
        self.sub_codes: List[str] = []
        self.sub_map: Mapping[str, List[TimeSeriesSubscriber]] = {}


class TimeSeries(object):
    """
    TimeSeriesFunction封装了跟具体的数据供应商的交互逻辑，TimeSeries在其基础上增加了本地缓存的逻辑，本地缓存记录在DataRecord中。
    """

    def __init__(self, name: str = None, data_record: Mapping[str, DataRecord] = None):
        self.name = name
        if data_record:
            self.data_record = data_record
        else:
            self.data_record = {}
        self.tp: TimeSeriesType = None

    def with_type(self, tp: TimeSeriesType):
        self.name = tp.name()
        self.tp = tp

    def current_price(self, codes: List[str], time: Timestamp = None) -> Mapping[str, CurrentPrice]:
        """
        如果time不为空，则认为是在回测环境中获取实时数据
        :param codes:
        :param time:
        :return:
        """
        if not time:
            if not isinstance(self.tp, RTTimeSeriesType):
                raise RuntimeError("实时价格需要从实时时序类型获取")
            return self.tp.current_price(codes)
        else:
            if not isinstance(self.tp, HistoryTimeSeriesType):
                raise RuntimeError("历史的实时价格需要从历史的时序类型获取")
            return self.tp.current_price_in_history(codes, time, self)

    def history_data(self, command: HistoryDataQueryCommand, from_local: bool = False,
                     remove_duplicated: bool = True) -> DataFrame:
        """
        返回DataFrame， index为[visible_time, code]
        :param command:
        :param from_local:
        :return:
        """
        if not isinstance(self.tp, HistoryTimeSeriesType):
            raise RuntimeError("不支持获取历史数据")
        if not self.tp.should_cache() and from_local:
            logging.warning("该时序类型不支持缓存，将从服务器获取")
            from_local = False
        ts_data_list: List[TSData] = []
        if not from_local:
            ts_data_list = self.tp.load_history_data(command)
        else:
            if not self.is_local_cached(command):
                logging.info("本地数据没有缓存，将会下载")
                self.download_data(command)
            ts_data_repo: TSDataRepo = BeanContainer.getBean(TSDataRepo)
            ts_data_list = ts_data_repo.query(self.name, command)

        # change to DataFrame, MultiIndex (visible_time, code)
        df_data = []
        for ts_data in ts_data_list:
            df_data.append(ts_data.to_dict())

        df = DataFrame(data=df_data).set_index(['visible_time', 'code'])
        # 去重
        if remove_duplicated:
            df = df[~df.index.duplicated()]
        # 由于下载是批量下载，所以下载的数据可能比想要下载的数据要多
        if command.start and command.end:
            return df.sort_index(level=0).loc[command.start: command.end]
        else:
            df = df.sort_index(level=0)
            return df.loc[df.index.get_level_values(0)[-command.window:]]

    def subscribe(self, subscriber: TimeSeriesSubscriber, codes: List[str]):
        if not isinstance(self.tp, RTTimeSeriesType):
            raise RuntimeError("不支持订阅")
        self.tp.sub_func(subscriber, codes)

    def unsubscribe(self, subscriber: TimeSeriesSubscriber, codes: List[str]):
        if not isinstance(self.tp, RTTimeSeriesType):
            raise RuntimeError('不支持反订阅')
        self.tp.unsub_func(subscriber, codes)

    def download_data(self, command: HistoryDataQueryCommand):
        if not isinstance(self.tp, HistoryTimeSeriesType):
            raise RuntimeError("非法的tsType")
        increment_commands = []
        commands: List[SingleCodeQueryCommand] = command.to_single_code_command()
        for command in commands:
            if command.code in self.data_record:
                increment_commands.extend(command.minus(self.data_record[command.code]))
            else:
                increment_commands.append(command)

        total_count = 0
        for i_command in increment_commands:

            data_list: List[TSData] = self.tp.load_history_data(i_command)
            ts_data_repo: TSDataRepo = BeanContainer.getBean(TSDataRepo)
            ts_data_repo.save(data_list)
            if i_command.code in self.data_record:
                self.data_record[i_command.code].update(i_command)
            else:
                self.data_record[i_command.code] = DataRecord(i_command.code, i_command.start, i_command.end)
            self.save()
            total_count += len(data_list)

        logging.info("下载完成， 共下载了{}个数据".format(total_count))

    def save(self):
        time_series_repo: TimeSeriesRepo = BeanContainer.getBean(TimeSeriesRepo)
        time_series_repo.save(self)

    def is_local_cached(self, command: HistoryDataQueryCommand):
        increment_commands = []
        commands: List[SingleCodeQueryCommand] = command.to_single_code_command()
        for command in commands:
            if command.code in self.data_record:
                increment_commands.extend(command.minus(self.data_record[command.code]))
            else:
                increment_commands.append(command)
        return len(increment_commands) <= 0


class TSTypeRegistry(object):
    types: Dict[str, TimeSeriesType] = {}

    @classmethod
    def find_function(cls, ts_type_name: str) -> TimeSeriesType:
        return cls.types[ts_type_name]

    @classmethod
    def register(cls, func: TimeSeriesType):
        name = func.name()
        if not name:
            raise RuntimeError("name 必须")
        if name in cls.types:
            raise RuntimeError("重复注册")
        cls.types[name] = func


class TimeSeriesRepo(metaclass=ABCMeta):
    @abstractmethod
    def find_one(self, name):
        pass

    @abstractmethod
    def save(self, ts: TimeSeries):
        pass


class TSDataRepo(metaclass=ABCMeta):
    @abstractmethod
    def save(self, ts_data_list: List[TSData]):
        pass

    @abstractmethod
    def query(self, ts_name, command):
        pass
