from se2.domain.time_series import *
from se2.domain.common import *
from se2.infras.ib2 import *

import pandas as pd

ts_repo: TimeSeriesRepo = BeanContainer.getBean(TimeSeriesRepo)
# ts: TimeSeries = ts_repo.find_one("ibMinBar")


start = pd.Timestamp("2020-06-30 21:30:00", tz='Asia/Shanghai')
end = pd.Timestamp("2020-12-31 05:00:00", tz='Asia/Shanghai')
codes = ["SPCE_STK_USD_SMART"]
command = HistoryDataQueryCommand(start, end, codes)

# ts:TimeSeries = ts_repo.find_one("ibTrade")
# df = ts.history_data(command, remove_duplicated=False)
# df = df[df['size']>10]
# df = df.droplevel(level=1)

# bid ask 价格变化
ts:TimeSeries = ts_repo.find_one("ibMinBar")
# ts.download_data(command)

# df = ts.history_data(command, from_local=True)
# ab_df = ts.history_data(command, remove_duplicated=False)
# ab_df = ab_df.droplevel(level=1)
# # 画图
# # ax = df[['price']].plot()
# s = (ab_df['ask_price'] - ab_df['bid_price'])
# s.groupby(pd.Grouper(freq='1Min')).describe()['mean'].plot()
#
print("done")