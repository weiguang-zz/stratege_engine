import os
os.environ['config.dir'] = "/Users/zhang/PycharmProjects/strategy_engine_v2/interface"
# 如果没有日志目录的话，则创建
if not os.path.exists("log"):
    os.makedirs("log")

from se import BeanContainer, TimeSeriesRepo
from se.domain2.time_series.time_series import TimeSeries, TimeSeriesSubscriber, TSData


ts_repo: TimeSeriesRepo = BeanContainer.getBean(TimeSeriesRepo)
ts: TimeSeries = ts_repo.find_one("ibTick")
class TestSubscriber(TimeSeriesSubscriber):

    def on_data(self, data: TSData):
        import logging
        logging.info("data:{}".format(data.__dict__))


ts.subscribe(TestSubscriber(), ["GME_STK_USD_SMART"])
print("done")