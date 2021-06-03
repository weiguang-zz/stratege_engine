import pandas as pd
from trading_calendars import get_calendar

from se2.domain.engine import *
from new_interface.strategy_spce.st import SPCEStrategy

code = 'SPCE_STK_USD_SMART'

data_portal = DataPortal(True, "ibAdjustedDailyBar", [code])
acc = BacktestAccount("bt_spce2", 10000, data_portal)

engine = Engine(acc, data_portal)
scope = Scope(["SPCE_STK_USD_SMART"], trading_calendar=get_calendar("NYSE"))
strategy = SPCEStrategy(scope)

start = pd.Timestamp('2020-01-01', tz='Asia/Shanghai')
end = pd.Timestamp('2021-01-01', tz='Asia/Shanghai')
res = engine.run_backtest(strategy, start, end, 'ibAdjustedDailyBar')

print('done')