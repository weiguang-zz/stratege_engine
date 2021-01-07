import pandas as pd
from pandas._libs.tslibs.timestamps import Timestamp

from se.domain2.time_series.time_series import HistoryDataQueryCommand, TimeSeriesRepo

start = pd.Timestamp("2020-01-01", tz='Asia/Shanghai')
end = pd.Timestamp("2020-01-03", tz='Asia/Shanghai')
command = HistoryDataQueryCommand(start, end, ['TSLA_STK_USD_SMART'])
ts = TimeSeriesRepo.find_one("ibMinBar")
df = ts.download_data(command)



