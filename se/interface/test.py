import pandas as pd
from pandas._libs.tslibs.timestamps import Timestamp

from se.domain2.pipeline.pipeline import Pipeline, Domain
from se.domain2.time_series.time_series import HistoryDataQueryCommand, TimeSeriesRepo

start = pd.Timestamp("2020-01-01", tz='Asia/Shanghai')
end = pd.Timestamp("2020-01-03", tz='Asia/Shanghai')
# command = HistoryDataQueryCommand(start, end, ['TSLA_STK_USD_SMART'])
# ts = TimeSeriesRepo.find_one("ibMinBar")
# # ts.download_data(command)
#
# df = ts.history_data(command, True)

# print("done")


# test pipeline
from se.domain2.pipeline.pipeline import *
columns = {
    "close": LoadableColumn("ibMinBar", "close"),
    "close_ma5": MovingAverageComputableColumn(in_params=[LoadableColumn("ibMinBar", "close")], window=5)
}

pipeline = Pipeline(domain=Domain(codes=["TSLA_STK_USD_SMART"], start=start, end=end), columns=columns, screen=None)
df = run_pipeline(pipeline)

print("done")







