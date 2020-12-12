from unittest import TestCase

from main.domain.account import AbstractAccount, Position, Order, OrderType, OrderDirection
from main.domain.data_portal import TSDataReader, Bar, DataPortal

from main.domain.engine import AbstractStrategy, BacktestAccount, AbstractMatchService
from main.domain.event_producer import Event, EventType, TimeEventType, EventProducer, TimeEventProducer, DateRules, \
    TimeRules
from typing import List, Dict
from trading_calendars import get_calendar
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
    )


class SimpleTestStrategy(AbstractStrategy):
    """
    一个简单的策略，买入苹果并且一直持有

    """
    def on_event(self, event: Event, account: AbstractAccount, data_portal: DataPortal,
                  match_service: AbstractMatchService):
        code = "TSLA_STK_USD_SMART"
        if event.event_type == EventType.TIME and event.sub_type == 'market_open':
            # 判断账户是否有持仓
            positions: Dict[str, Position] = account.get_positions()
            if len(positions) <= 0:
                # 买入苹果
                recent_price = match_service.latest_prices([code])
                # df: DataFrame = data_portal.history("ibHistory", "ib1MinBar", [code], 1)
                logging.info("当前时间:{current_time}, 获取到资产{code}的最新的价格数据为{recent_price}".
                             format(current_time=event.visible_time, code=code, recent_price=recent_price))
                quantity = int(account.cash / recent_price)
                order = Order(code, quantity, OrderType.MKT, OrderDirection.BUY, event.visible_time)
                account.place_order(order)
        elif event.event_type == EventType.ACCOUNT:
            logging.info("account event:" + str(event))

    def __init__(self):
        calendar = get_calendar("NYSE")
        ep = TimeEventProducer(DateRules.every_day(), TimeRules.market_open(calendar=calendar, offset=1)
                               , sub_type="market_open")
        super(SimpleTestStrategy, self).__init__([ep], calendar)





class Test(TestCase):

    def test_run_backtest(self):
        from main.service.strategy_service import run_backtest
        strategy = SimpleTestStrategy()
        account: BacktestAccount = run_backtest(strategy, "2020-01-01", "2020-01-30", 100000)
        # apple 2020-09-01 open 132.53
        # 2020-09-30 close 115.61
        print("done")
        # self.assertEquals(87242.32, result.net_value)
