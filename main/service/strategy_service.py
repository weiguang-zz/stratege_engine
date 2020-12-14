from main.domain.account import AbstractAccount, BacktestAccount
from pandas import Timestamp

from main.domain.data_portal import CurrentPriceLoader
from main.domain.engine import AbstractStrategy, StrategyEngine, AbstractMatchService


def run_backtest(strategy: AbstractStrategy, match_service: AbstractMatchService,
                 current_price_loader: CurrentPriceLoader,
                 start: str, end: str, initial_cash: float, tz='Asia/Shanghai'):
    engine: StrategyEngine = StrategyEngine(match_service)
    start = Timestamp(start, tz=tz)
    end = Timestamp(end, tz=tz)
    if start > end:
        raise RuntimeError("start 不能大于 end")
    return engine.run_backtest(strategy, current_price_loader, start, end, initial_cash)




def run(strategy: AbstractStrategy, account: AbstractAccount):
    pass