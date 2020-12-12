from main.domain.account import AbstractAccount, BacktestAccount
from pandas import Timestamp

from main.domain.engine import AbstractStrategy, StrategyEngine, IBAmericaMinBarMatchService


def run_backtest(strategy: AbstractStrategy, start: str, end: str, initial_cash: float, tz='Asia/Shanghai'):
    engine: StrategyEngine = StrategyEngine()
    start = Timestamp(start, tz=tz)
    end = Timestamp(end, tz=tz)
    if start > end:
        raise RuntimeError("start 不能大于 end")
    account = BacktestAccount(initial_cash=initial_cash)
    match_service = IBAmericaMinBarMatchService(strategy.trading_calendar, start, end, account)
    return engine.run_backtest(strategy, match_service)




def run(strategy: AbstractStrategy, account: AbstractAccount):
    pass