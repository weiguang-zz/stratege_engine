import time
from configparser import ConfigParser

import pandas as pd
# from pandas._libs.tslibs.timestamps import Timestamp

# from se.domain2.pipeline.pipeline import Pipeline, Domain
# from se.domain2.time_series.time_series import HistoryDataQueryCommand, TimeSeriesRepo
#
# start = pd.Timestamp("2020-01-01", tz='Asia/Shanghai')
# end = pd.Timestamp("2020-01-05", tz='Asia/Shanghai')
# command = HistoryDataQueryCommand(start, end, ['TSLA_STK_USD_SMART'])
# ts = BeanContainer.getBean(TimeSeriesRepo).find_one("ibMinBar")
# ts.download_data(command)
#
# df = ts.history_data(command, True)
# from se.infras.models import AccountModel
# import numpy as np
# account = AccountModel.objects(name='test13').first()
# history_net_value_df = pd.Series(account.history_net_value)
# pnls = [op.pnl for op in account.history_operations]
# pnls_p = [(op.pnl)/(op.start_cash) for op in account.history_operations]
# # 胜率
# profits = np.array(pnls_p)[np.array(pnls_p) > 0]
# loss = np.array(pnls_p)[np.array(pnls_p) < 0]
# p_rate = len(profits) / len(pnls_p)
# l_rate = len(loss) / len(pnls_p)
# # 平均盈利
# p_mean = profits.mean()
# l_mean = loss.mean()

import pandas as pd
import numpy as np

# from se.infras.models import AccountModel
#
# account = AccountModel.objects(name='test13').first()
# history_net_value_df = pd.Series(account.history_net_value)
# pnls = [op.pnl for op in account.history_operations]
# pnls_p = [(op.pnl)/(op.start_cash) for op in account.history_operations]
# # 胜率
# profits = np.array(pnls_p)[np.array(pnls_p) > 0]
# loss =  np.array(pnls_p)[np.array(pnls_p) < 0]
# p_rate = len(profits) / len(pnls_p)
# l_rate = len(loss) / len(pnls_p)
# # 平均盈利
# p_mean = profits.mean()
# l_mean = loss.mean()
# print("profit_rate:{}, loss_rate:{}, profit_mean{}, loss_mean:{}", p_rate, l_rate, p_mean, l_mean)

# from se.domain2.time_series.time_series import HistoryDataQueryCommand, TimeSeriesRepo
#
# start = pd.Timestamp("2020-01-01", tz='Asia/Shanghai')
# end = pd.Timestamp("2020-12-01", tz='Asia/Shanghai')
# command = HistoryDataQueryCommand(start, end, ['SPCE_STK_USD_SMART'])
# ts = BeanContainer.getBean(TimeSeriesRepo).find_one("ibMinBar")
# df = ts.history_data(command, from_local=True)
# df = df.droplevel(level=1)
# # 注意origin的tz必须跟原始的DataFrame的DatetimeIndex的timezone相同
# df = df.resample('1D', origin=pd.Timestamp("2020-06-01 21:00:00", tz='Asia/Shanghai'))\
#     .agg({'open': 'first', 'close': 'last'})
# intra_day_rets = (df['close'] - df['open']) / df['open']
# intra_day_rets = intra_day_rets.dropna()
#
# rets = intra_day_rets.values
# profit_rets = rets[rets > 0]
# loss_rets = rets[rets < 0]
# profit_rate = float(len(profit_rets)) / len(rets)
# loss_rate = float(len(loss_rets)) / len(rets)
# profit_mean = profit_rets.mean()
# # loss_mean = loss_rets.mean()
# from pandas._libs.tslibs.timestamps import Timestamp
#
# from se.domain2.account.account import MKTOrder, OrderDirection
# from se.infras.models import AccountModel
#
# # am = AccountModel.objects(name='test14').first()
# od = OrderDirection(1)
# print(type(MKTOrder("", OrderDirection.BUY, 199, Timestamp.now())).__name__)
#
# print("done")

# test pipeline
# from se.domain2.pipeline.pipeline import *
# columns = {
#     "close": LoadableColumn("ibMinBar", "close"),
#     "close_ma5": MovingAverageComputableColumn(in_params=[LoadableColumn("ibMinBar", "close")], window=5)
# }
#
# pipeline = Pipeline(domain=Domain(codes=["TSLA_STK_USD_SMART"], start=start, end=end), columns=columns, screen=None)
# df = run_pipeline(pipeline)
#
# print("done")

import ibapi.wrapper
import logging

# import yaml
# import sys
#
# dt = yaml.load(open("log.yaml"), Loader=yaml.SafeLoader)
#
# import logging.config
# logging.config.dictConfig(dt, )
#
# log = logging.getLogger("ibapi.wrapper")
# log.info("哈哈")
# logging.info("hahaha")
# print("done")

# logging.basicConfig(level=logging.INFO)
# logging.info("哈哈")
# import logging.config
#
# import yaml
#
# logging.config.dictConfig(yaml.load(open("log.yaml"), Loader=yaml.SafeLoader))
# logging.info("哈哈")
# from pandas._libs.tslibs.timestamps import Timestamp
# from trading_calendars import get_calendar
#
# from se.domain2.engine.engine import MarketClose
#
# market_close = MarketClose(second_offset=-5)
# print(market_close.next_time(get_calendar("NYSE"), Timestamp("2021-01-26 04:59:56", tz='Asia/Shanghai')).tz_convert("Asia/Shanghai"))
# from se.domain2.account.account import AccountRepo, AbstractAccount
# from se.domain2.domain import BeanContainer
# import se.infras
#
# repo:AccountRepo = BeanContainer.getBean(AccountRepo)
# acc: AbstractAccount = repo.find_one("test25")
# operations = acc.history_operations()

# config_file_name = 'config_default.ini'
# config = ConfigParser()
# config.read(config_file_name)
# # print(config.get("email", 'activate'))
# from pandas._libs.tslibs.timestamps import Timestamp
#
# from se.domain2.account.account import OrderDirection, MKTOrder
# from se.infras.ib import IBAccount
#
# acc = IBAccount("test50", 10000).with_client(config.get('ib', 'host'), config.getint('ib', 'port'),
#                                              config.getint('ib', 'client_id'))
# order = MKTOrder("GSX_STK_USD_SMART", direction=OrderDirection.BUY, quantity=200,
#                  place_time=Timestamp.now(tz='Asia/Shanghai'))
# acc.place_order(order)

# 查询数据
# from pandas._libs.tslibs.timestamps import Timestamp
# import se.infras
#
# from se.domain2.domain import BeanContainer
# from se.domain2.time_series.time_series import TimeSeriesRepo, TimeSeries, HistoryDataQueryCommand
#
# repo: TimeSeriesRepo = BeanContainer.getBean(TimeSeriesRepo)
# ts: TimeSeries = repo.find_one("ibMinBar")
# comm = HistoryDataQueryCommand(start=Timestamp('2019-06-17 21:30:00', tz='Asia/Shanghai'),
#                         end=Timestamp('2019-06-18 00:30:00', tz='Asia/Shanghai'),
#                         codes=['GSX_STK_USD_SMART'])
#
# while True:
#     try:
#
#         data = ts.history_data(comm, from_local=False)
#         time.sleep(10)
#     except:
#         logging.error("error")
#         time.sleep(10)
# print("done")

import logging
logging.basicConfig(level=logging.INFO)

while True:
    logging.info("time:{}".format(time.strftime("%Y-%m-%d %H:%M:%S")))

    time.sleep(2)