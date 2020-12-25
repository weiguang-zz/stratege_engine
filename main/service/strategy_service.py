from pandas import Timestamp

from main.domain.account import AbstractAccount
from main.domain.data_portal import CurrentPriceLoader
from main.domain.engine import AbstractStrategy, StrategyEngine


def run_backtest(strategy: AbstractStrategy, match_service,
                 current_price_loader: CurrentPriceLoader,
                 start: str, end: str, initial_cash: float, tz='Asia/Shanghai'):
    engine: StrategyEngine = StrategyEngine(match_service)
    start = Timestamp(start, tz=tz)
    end = Timestamp(end, tz=tz)
    if start > end:
        raise RuntimeError("start 不能大于 end")
    return engine.run_backtest(strategy, current_price_loader, start, end, initial_cash)