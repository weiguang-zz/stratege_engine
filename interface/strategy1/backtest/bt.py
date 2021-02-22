import os
os.environ['config.dir'] = "/Users/zhang/PycharmProjects/strategy_engine_v2/interface"
# 如果没有日志目录的话，则创建
if not os.path.exists("log"):
    os.makedirs("log")

from se import BeanContainer, AccountRepo
from se.domain2.account.account import AbstractAccount
import pandas as pd
from trading_calendars import get_calendar

from se.domain2.engine.engine import Engine, Scope
from strategies.strategy import TestStrategy2

engine = Engine()
scope = Scope(["SPCE_STK_USD_SMART"], trading_calendar=get_calendar("NYSE"))
strategy = TestStrategy2(scope)

# 回测
start = pd.Timestamp("2021-02-01", tz='Asia/Shanghai')
end = pd.Timestamp("2021-02-18", tz='Asia/Shanghai')
result = engine.run_backtest(strategy, start, end, 10000, "test60", "ibAdjustedDailyBar")
print("done")

# 查看结果
# repo: AccountRepo = BeanContainer.getBean(AccountRepo)
# account: AbstractAccount = repo.find_one("test56")
# s = pd.Series(account.history_net_value)
# s.plot()
# print("done")
