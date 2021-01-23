from abc import *
from typing import *

from pandas import DatetimeIndex, DataFrame
import pandas as pd
from pandas._libs.tslibs.timestamps import Timestamp

from se.domain2.time_series.time_series import TimeSeries, HistoryDataQueryCommand, TimeSeriesRepo


class Domain(object):

    def __init__(self, codes: List[str], start: Timestamp, end: Timestamp):
        self.codes = codes
        self.start = start
        self.end = end


class Column(metaclass=ABCMeta):
    @abstractmethod
    def get_data(self, domain: Domain):
        pass


class LoadableColumn(Column):

    def get_data(self, domain: Domain):
        ts: TimeSeries = BeanContainer.getBean(TimeSeriesRepo).find_one(self.ts_type_name)
        df = ts.history_data(HistoryDataQueryCommand(domain.start, domain.end, domain.codes))
        return df[self.ts_type_column_name].unstack()[domain.codes]

    def __init__(self, ts_type_name: str, ts_type_column_name: str):
        self.ts_type_name = ts_type_name
        self.ts_type_column_name = ts_type_column_name


class ComputableColumn(Column, metaclass=ABCMeta):

    @abstractmethod
    def calc(self, *args):
        pass

    def __init__(self, in_params: List[Column]):
        self.in_params = in_params


class MovingAverageComputableColumn(ComputableColumn):

    def get_data(self, domain: Domain):
        args = []
        for param in self.in_params:
            df = param.get_data(domain)
            args.append(df)
        return self.calc(*args)

    def calc(self, df):
        return df.rolling(self.window).mean()

    def __init__(self, in_params: List[Column], window: int):
        super().__init__(in_params)
        if len(in_params) != 1:
            raise RuntimeError("非法的入参")
        self.window = window


class Pipeline(object):
    def __init__(self, domain: Domain, columns: Mapping[str, Column], screen: Column = None):
        self.domain = domain
        self.columns = columns
        self.screen = screen


def run_pipeline(pipeline: Pipeline):
    column_values = []
    column_names = []
    for column_name in pipeline.columns.keys():
        values: DataFrame = pipeline.columns[column_name].get_data(pipeline.domain)
        column_values.append(values.stack(dropna=False))
        column_names.append(column_name)
    df = pd.concat(column_values, axis=1)
    df.columns = column_names
    return df
