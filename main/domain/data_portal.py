from datetime import datetime
from typing import *

import requests
from pandas import Timestamp, DataFrame


class Bar(object):

    def __init__(self, code, time, open, high, low, close, volume):
        self.code = code
        self.time = time
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume

    def __lt__(self, other):
        return self.time < other.place_time


class TSDataLoader(object):
    def __init__(self, data_provider_name: str, ts_type_name: str):
        self.data_provider_name = data_provider_name
        self.ts_type_name = ts_type_name

    def history_data_in_backtest(self, codes: List[str], end_time: Timestamp,
                                 count=100):

        pass

    def current_price_in_backtest(self, codes, end_time: Timestamp):

        pass

    def history_data(self, codes: List[str], end_time: Timestamp, count=100):

        pass

    def current_price(self, codes: List[str]):

        pass





class DefaultTSDataReader(object):

    def __init__(self, data_provider_name: str, ts_type_name: str):
        self.data_provider_name = data_provider_name
        self.ts_type_name = ts_type_name

    def get_ts_data(self, codes: List[str], start: Timestamp = None, end: Timestamp = Timestamp.now(tz='Asia/Shanghai'),
                    count=100) -> DataFrame:
        """
        获取时序数据，优先按照开始和结束时间查询。 其次按照结束时间和数量进行查询
        这里的所有时间都是针对visible_time
        :param count:
        :param codes:
        :param start:
        :param end:
        :return: DataFrame, 索引为visible_time + code
        """
        if len(codes) <= 0:
            raise RuntimeError("code不能为空")
        if start:
            start = start.tz_convert("Asia/Shanghai")
        end = end.tz_convert("Asia/Shanghai")
        pattern = '%Y-%m-%d %H:%M:%S'
        if start and end:
            params = {
                "providerName": self.data_provider_name,
                "tsTypeName": self.ts_type_name,
                "codes": ",".join(codes),
                "startTime": datetime.strftime(start, pattern),
                "endTime": datetime.strftime(end, pattern)
            }
        else:
            params = {
                "providerName": self.data_provider_name,
                "tsTypeName": self.ts_type_name,
                "codes": ",".join(codes),
                "endTime": datetime.strftime(end, pattern),
                "count": count
            }
        url = "http://localhost:32900/queryData"
        resp = requests.get(url, params)
        if resp.status_code == 200:
            result = resp.json()
            if not result['success']:
                raise RuntimeError("下载数据出错，错误信息：" + result['errorMsg'])
            else:
                df_data = []
                for single_data in result['data']:
                    m_data = {'visible_time': Timestamp(single_data['visiableTime']), 'code': single_data['code']}
                    m_data.update(single_data['values'])
                    df_data.append(m_data)
                df = DataFrame(df_data)
                df.set_index(['visible_time', 'code'], inplace=True)
                return df


class DataPortal(object):
    """
    在回测或者实盘中，策略要获取实时数据或者历史数据都从这里获取。 实时数据跟历史数据的区别已经被数据中心封装了，数据中心会接收实时数据流，
    并将其保存下来
    """

    def __init__(self, is_real_time: bool = False):
        self.current_dt = None
        self.is_real_time = is_real_time

    def set_current_dt(self, dt: Timestamp):
        if dt.second != 0:
            raise RuntimeError("当前时间只能是分钟的开始")
        self.current_dt = dt

    def history(self, provider_name: str, ts_type_name: str, codes: List[str], window: int):
        if not self.current_dt:
            raise RuntimeError("当前时间没有设置")
        ts_data_reader = TSDataReader(data_provider_name=provider_name, ts_type_name=ts_type_name)
        return ts_data_reader.get_ts_data(codes, end=self.current_dt, count=window)

    def current_price(self, provider_name: str, ts_type_name: str, codes: List[str]):
        pass