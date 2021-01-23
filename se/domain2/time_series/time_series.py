from typing import *

from cassandra.cqlengine.query import ModelQuerySet, BatchQuery
from pandas import Timestamp, DataFrame
from abc import *
import json
import logging

from se.domain2.domain import BeanContainer

class Price(object):

    def __init__(self, code, price, time: Timestamp):
        self.code = code
        self.price = price
        self.time = time

class Column(object):

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


class DataRecord(object):
    def __init__(self, code: str, start: Timestamp, end: Timestamp):
        self.code = code
        self.start = start.tz_convert("Asia/Shanghai")
        self.end = end.tz_convert("Asia/Shanghai")

    def update(self, command):
        if not isinstance(command, SingleCodeQueryCommand):
            raise RuntimeError("wrong type")
        if command.code != self.code:
            raise RuntimeError("wrong code")
        if command.start < self.start:
            self.start = command.start
        if command.end > self.end:
            self.end = command.end



class HistoryDataQueryCommand(object):
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

    def to_single_code_command(self):
        commands: List[SingleCodeQueryCommand] = []
        for code in self.codes:
            commands.append(SingleCodeQueryCommand(self.start, self.end, code, self.window))
        return commands


class SingleCodeQueryCommand(HistoryDataQueryCommand):
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


class Asset(object):
    def __init__(self, code: str, tp: str):
        self.code = code
        self.tp = tp


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


class TimeSeriesDataRepo(object):
    @classmethod
    def save(cls, data_list: List[TSData]):
        time_series_repo = BeanContainer.getBean(TimeSeriesRepo)
        time_series_repo.save_ts(data_list)



    @classmethod
    def query(cls, ts_type_name: str, command: HistoryDataQueryCommand) -> List[TSData]:
        time_series_repo: TimeSeriesRepo = BeanContainer.getBean(TimeSeriesRepo)
        return time_series_repo.query_ts_data(ts_type_name, command)



class Subscription(object):
    def __init__(self, ts_type_name: str, code: str):
        self.ts_type_name = ts_type_name
        self.code = code


class TimeSeriesSubscriber(metaclass=ABCMeta):
    @abstractmethod
    def on_data(self, data: TSData):
        pass


class TimeSeriesFunction(metaclass=ABCMeta):
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def load_history_data(self, command: HistoryDataQueryCommand) -> List[TSData]:
        pass

    @abstractmethod
    def current_price(self, codes) -> Mapping[str, Price]:
        pass

    @abstractmethod
    def load_assets(self) -> List[Asset]:
        pass

    @abstractmethod
    def sub_func(self, subscription: Subscription):
        pass

    @abstractmethod
    def unsub_func(self, subscription: Subscription):
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



class TimeSeries(object):

    def __init__(self, name: str = None, data_record: Mapping[str, DataRecord] = None):
        self.name = name
        if data_record:
            self.data_record = data_record
        else:
            self.data_record = {}

    def with_func(self, func: TimeSeriesFunction):
        self.name = func.name()
        self.func = func
        # self.history_data_loader = func.load_history_data
        # self.assets_loader = func.load_assets
        # self.sub_func = func.sub_func
        # self.unsub_func = func.unsub_func
        column_dict = {}
        for column in func.columns():
            column_dict[column.name] = column
        self.column_dict = column_dict

    def history_data(self, command: HistoryDataQueryCommand, from_local: bool = False) -> DataFrame:
        ts_data_list: List[TSData] = []
        if not from_local:
            ts_data_list = self.func.load_history_data(command)
        else:
            if not self.is_local_cached(command):
                logging.info("本地数据没有缓存，将会下载")
                self.download_data(command)
            ts_data_list = TimeSeriesDataRepo.query(self.name, command)

        # change to DataFrame, MultiIndex (visible_time, code)
        df_data = []
        for ts_data in ts_data_list:
            df_data.append(ts_data.to_dict())

        df = DataFrame(data=df_data).set_index(['visible_time', 'code'])
        # 由于下载是批量下载，所以下载的数据可能比想要下载的数据要多
        if command.start and command.end:
            return df.sort_index(level=0).loc[command.start: command.end]
        else:
            df = df.sort_index(level=0)
            return df.loc[df.index.get_level_values(0)[-command.window:]]

    def subscribe(self, subscriber: TimeSeriesSubscriber, codes: List[str]):
        pass

    def unsubscribe(self, subscriber: TimeSeriesSubscriber, codes: List[str]):
        pass

    def on_data(self, data: TSData):
        """
        当从数据供应商获取到实时数据流时，该方法被回调
        :param data:
        :return:
        """
        pass

    def download_data(self, command: HistoryDataQueryCommand):
        increment_commands = []
        commands: List[SingleCodeQueryCommand] = command.to_single_code_command()
        for command in commands:
            if command.code in self.data_record:
                increment_commands.extend(command.minus(self.data_record[command.code]))
            else:
                increment_commands.append(command)

        total_count = 0
        for i_command in increment_commands:
            data_list: List[TSData] = self.func.load_history_data(i_command)
            TimeSeriesDataRepo.save(data_list)
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


    def current_price(self, codes: List[str]) -> Mapping[str, Price]:
        return self.func.current_price(codes)

    def is_local_cached(self, command: HistoryDataQueryCommand):
        increment_commands = []
        commands: List[SingleCodeQueryCommand] = command.to_single_code_command()
        for command in commands:
            if command.code in self.data_record:
                increment_commands.extend(command.minus(self.data_record[command.code]))
            else:
                increment_commands.append(command)
        return len(increment_commands) <= 0


class TSFunctionRegistry(object):
    funcs: Mapping[str, TimeSeriesFunction] = {}

    @classmethod
    def find_function(cls, ts_type_name: str) -> TimeSeriesFunction:
        return cls.funcs[ts_type_name]

    @classmethod
    def register(cls, func: TimeSeriesFunction):
        name = func.name()
        if not name:
            raise RuntimeError("name 必须")
        if name in cls.funcs:
            raise RuntimeError("重复注册")
        cls.funcs[name] = func


class TimeSeriesRepo(metaclass=ABCMeta):
    @abstractmethod
    def find_one(self, name):
        pass

    @abstractmethod
    def save(self, ts: TimeSeries):
        pass

    @abstractmethod
    def save_ts(self, ts_list: List[TSData]):
        pass

    @abstractmethod
    def query_ts_data(self, ts_type_name, command):
        pass
