from abc import *
from typing import *

from pandas import DatetimeIndex, DataFrame
import pandas as pd
import numpy as np
from pandas._libs.tslibs.timestamps import Timestamp

from se.domain2.time_series.time_series import TimeSeries, HistoryDataQueryCommand


class Domain(object):

    def __init__(self, codes: List[str], start: Timestamp, end: Timestamp):
        self.codes = codes
        self.start = start
        self.end = end


class Column(metaclass=ABCMeta):
    @abstractmethod
    def get_data(self, domain: Domain):
        pass

    def __init__(self, name: str):
        self.name = name


class LoadableColumn(Column):

    def get_data(self, domain: Domain):
        from se import ts_repo
        ts: TimeSeries = ts_repo.find_one(self.ts_type_name)
        df = ts.history_data(HistoryDataQueryCommand(domain.start, domain.end, domain.codes, -1))
        return df.unstack()

    def __init__(self, ts_type_name: str, ts_type_column_name: str):
        super().__init__(ts_type_column_name)
        self.ts_type_name = ts_type_name
        self.ts_type_column_name = ts_type_column_name


class ComputableColumn(Column, metaclass=ABCMeta):

    @abstractmethod
    def calc(self, *args):
        pass

    def __init__(self, name: str, in_params: List[Column]):
        super().__init__(name)
        self.in_params = in_params


class MovingAverageComputableColumn(ComputableColumn):

    def get_data(self, domain: Domain):
        args = []
        for param in self.in_params:
            df = param.get_data(domain)
            args.append(df)
        return self.calc(*args)

    def calc(self, df):
        return df.rolling(self.window).mean().values

    def __init__(self, name: str, in_params: List[Column], window: int):
        super().__init__(name, in_params)
        if len(in_params) != 1:
            raise RuntimeError("非法的入参")
        self.window = window


class Pipeline(object):
    def __init__(self, domain: Domain, columns: List[Column], screen: Column = None):
        self.domain = domain
        self.columns = columns
        self.screen = screen


def run_pipeline(pipeline: Pipeline):
    column_values = []
    for column in pipeline.columns:
        values: DataFrame = column.get_data(pipeline.domain)
        column_values.append(values.stack())
    df = pd.concat(column_values, axis=1)
    df.columns = pipeline.domain.codes
    return df
