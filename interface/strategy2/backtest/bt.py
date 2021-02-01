import pandas as pd
from trading_calendars import get_calendar

from se.domain2.engine.engine import Engine, Scope
from strategies.strategy import TestStrategy3

engine = Engine()
scope = Scope(["GSX_STK_USD_SMART"], trading_calendar=get_calendar("NYSE"))
strategy = TestStrategy3(scope)

# 回测
start = pd.Timestamp("2019-06-10", tz='Asia/Shanghai')
end = pd.Timestamp("2020-12-01", tz='Asia/Shanghai')
result = engine.run_backtest(strategy, start, end, 10000, "test32")
print("done")
