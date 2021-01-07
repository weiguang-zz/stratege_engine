from typing import *

from cassandra.cqlengine.query import ModelQuerySet, BatchQuery
from pandas import Timestamp, DataFrame
from abc import *
import json
import logging

from se.infras.models import TimeSeriesModel, DataRecordModel, TimeSeriesDataModel


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
        b = BatchQuery()
        for ts_data in data_list:
            func = TSFunctionRegistry.find_function(ts_data.ts_type_name)
            value_serialized: str = func.serialize(ts_data.values)
            TimeSeriesDataModel.batch(b).create(type=ts_data.ts_type_name, code=ts_data.code,
                                                visible_time=ts_data.visible_time, data=value_serialized)
        b.execute()

    @classmethod
    def query(cls, ts_type_name: str, command: HistoryDataQueryCommand) -> List[TSData]:
        data_list: List[TSData] = []
        if command.start and command.end:
            r: ModelQuerySet = TimeSeriesDataModel.objects.filter(type=ts_type_name, code__in=command.codes,
                                                                  visible_time__gte=command.start,
                                                                  visible_time__lte=command.end)
        else:
            r: ModelQuerySet = TimeSeriesDataModel.objects.filter(type=ts_type_name, code__in=command.codes,
                                                                  visible_time__lte=command.end) \
                .order_by("visible_time").limit(command.window)

        func = TSFunctionRegistry.find_function(ts_type_name)

        for row in r.all():
            values: Mapping[str, object] = func.deserialized(row.data)
            ts_data = TSData(row.type, row.visible_time, row.code, values)
            data_list.append(ts_data)
        return data_list


class Subscription(object):
    def __init__(self, ts_type_name: str, code: str):
        self.ts_type_name = ts_type_name
        self.code = code


class TimeSeriesSubscriber(metaclass=ABCMeta):
    @abstractmethod
    def on_data(self, data: TSData):
        pass


class DataRecord(object):
    def __init__(self, code: str, start: Timestamp, end: Timestamp):
        self.code = code
        self.start = start
        self.end = end


class TimeSeriesFunction(metaclass=ABCMeta):
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def load_history_data(self, command: HistoryDataQueryCommand) -> List[TSData]:
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
            ts_data_list = TimeSeriesDataRepo.query(command)

        # change to DataFrame, MultiIndex (visible_time, code)
        df_data = []
        for ts_data in ts_data_list:
            df_data.append(ts_data.to_dict())

        df = DataFrame(data=df_data).set_index(['visible_time', 'code'])
        # 由于下载是批量下载，所以下载的数据可能比想要下载的数据要多
        return df.sort_index(level=0).loc[command.start: command.end]

    def subscribe(self, subscriber: TimeSeriesSubscriber):
        pass

    def unsubscribe(self, subscriber: TimeSeriesSubscriber):
        pass

    def on_data(self, data: TSData):
        """
        当从数据供应商获取到实时数据流时，该方法被回调
        :param data:
        :return:
        """
        pass

    def download_data(self, command: HistoryDataQueryCommand):
        ts_data_list: List[TSData] = self.func.load_history_data(command)
        TimeSeriesDataRepo.save(ts_data_list)
        logging.info("下载完成， 共下载了{}个数据".format(len(ts_data_list)))



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


class TimeSeriesRepo(object):
    @classmethod
    def find_one(cls, name: str) -> TimeSeries:
        ts = TimeSeries()
        r: ModelQuerySet = TimeSeriesModel.objects(name=name)
        if r.count() == 1:
            model: TimeSeriesModel = r.first()
            data_record = {}
            for key in model.data_record.keys():
                dr_model: DataRecordModel = model.data_record[key]
                data_record[key] = DataRecord(dr_model.code, dr_model.start_time, dr_model.end_time)
            ts = TimeSeries(name=model.name, data_record=data_record)
        elif r.count() > 1:
            raise RuntimeError("wrong data")

        # 查找该实例的方法
        func: TimeSeriesFunction = TSFunctionRegistry.find_function(name)
        if not func:
            raise RuntimeError("没有找到实例方法")
        ts.with_func(func)
        return ts
