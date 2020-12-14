from unittest import TestCase

from pandas import DataFrame

from main.domain.account import AbstractAccount, Position, Order, OrderType, OrderDirection
from main.domain.data_portal import TSDataReader, DataPortal

from main.domain.engine import AbstractStrategy, BacktestAccount, AbstractMatchService
from main.domain.event_producer import Event, EventType, EventProducer, TimeEventProducer, DateRules, \
    TimeRules, TSDataEventProducer
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

    def on_event(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
        code = "TSLA_STK_USD_SMART"
        if event.event_type == EventType.TIME and event.sub_type == 'market_open':
            # 判断账户是否有持仓
            positions: Dict[str, Position] = account.get_positions()
            if len(positions) <= 0:
                # 买入苹果
                recent_price = data_portal.current_price([code])
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


class TestStrategy2(AbstractStrategy):
    """
    连续三个阴线后买入，并在涨2%后卖出，或者在跌1%后卖出
    """

    def on_event(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
        if event.event_type == EventType.TIME and event.sub_type == "market_open":
            self.minutes_bars = []

        if event.event_type == EventType.DATA and event.sub_type == "IB:minBar":
            self.minutes_bars.append(event.data)
            if len(account.positions) <= 0:
                if self.is_down(self.minutes_bars[-1]) and self.is_down(self.minutes_bars[-2]) and self.is_down(
                        self.minutes_bars[-3]):
                    current_price: Dict[str, float] = data_portal.current_price([self.code])
                    quantity = int(account.cash / current_price[self.code])
                    order = Order(order_type=OrderType.MKT, code=self.code, quantity=quantity,
                                  direction=OrderDirection.BUY, time=event.visible_time)
                    account.place_order(order)

                    sell_price = current_price[self.code] * 1.02
                    sell_order = Order(order_type=OrderType.LIMIT, code=self.code, quantity=quantity,
                                       direction=OrderDirection.SELL, time=event.visible_time, limit_price=sell_price)
                    account.place_order(sell_order)

                    stop_price = current_price[self.code] * 0.99
                    stop_order = Order(order_type=OrderType.STOP, code=self.code, quantity=quantity,
                                       direction=OrderDirection.SELL, time=event.visible_time, stop_price=stop_price)
                    account.place_order(stop_order)
            else:
                pass

    def __init__(self):
        calendar = get_calendar("NYSE")
        code = "TSLA_STK_USD_SMART"
        ep: EventProducer = TSDataEventProducer("IB", "minBar", [code])
        ep2: EventProducer = TimeEventProducer(DateRules.every_day(), TimeRules.market_open(offset=0), "market_open")
        self.minutes_bars = []
        self.code = code
        super(TestStrategy2, self).__init__([ep, ep2], calendar)

    @classmethod
    def is_down(cls, bar):
        if bar['close'] < bar['open']:
            return True
        else:
            return False

class TestStrategy3(AbstractStrategy):
    """
    网格交易，入场时机为股价下穿10日均线，买入50%，然后按照10%的幅度，上下各设置5个网格
    实现方法为： 每个交易日开始，根据当前价格和持仓，以及设定好的网格，计算要设置的限价单
    """

    def on_event(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
        if len(account.positions) <= 0:
            if event.event_type == EventType.TIME and event.sub_type == "before_trading_start":
                df: DataFrame = data_portal.history("IB", "dailyBar", [self.code], 10)
                self.avg_price = df['close'].avg()
            if event.event_type == EventType.TIME and event.sub_type == "market_open":
                current_price: Dict[str, float] = data_portal.current_price([self.code])
                if current_price < self.avg_price:
                    # 下单买入
                    quantity = int(account.cash*0.5 / current_price[self.code])
                    order = Order(order_type=OrderType.MKT, direction=OrderDirection.BUY, time=event.visible_time,
                                  quantity=quantity)
                    account.place_order(order)
                    self.base_price = current_price[self.code]

        else:
            if event.event_type == EventType.ACCOUNT and event.sub_type == "filled":
                positions = account.positions
                cp = data_portal.current_price([self.code])[self.code]
                p = (cp - self.base_price) / self.base_price
                p = int(p / 0.1)
                if p >= 5:
                    order = Order(self.code, quantity=positions[self.code], order_type=OrderType.MKT,
                                  direction=OrderDirection.SELL,
                                  time=event.visible_time)
                    account.place_order(order)

                #

                # 上一个网格的限价单
                dest_percentage = 0.5 - 0.1 * (p+1)
                next_price = ((p+1)*0.1 + 1) * self.base_price
                net_val = next_price * positions[self.code] + account.cash
                sell_quantity = int(positions[self.code] - (net_val * dest_percentage / next_price))
                if sell_quantity < 10:
                    # 如果订单数很少，说明当前价格是从上个网格下来的， 则挂上上个网格的限价单。
                    pass

                # 下一个网格限价单

        pass
    
    def __init__(self):
        calendar = get_calendar("NYSE")
        code = "TSLA_STK_USD_SMART"
        tep: EventProducer = TimeEventProducer(DateRules.every_day(), 
                                               TimeRules.market_open(offset=0, calendar=calendar), 
                                               "market_open")
        tep2 = TimeEventProducer(DateRules.every_day(), TimeRules.market_open(calendar=calendar, offset=-30),
                                 sub_type="before_trading_start")
        self.code = code
        self.avg_price = None
        self.base_price = None
        super(TestStrategy3, self).__init__([tep, tep2], calendar)
    



class Test(TestCase):

    def test_run_backtest(self):
        from main.service.strategy_service import run_backtest
        strategy = SimpleTestStrategy()
        account: BacktestAccount = run_backtest(strategy, "2020-01-01", "2020-01-30", 100000)
        # apple 2020-09-01 open 132.53
        # 2020-09-30 close 115.61
        print("done")
        # self.assertEquals(87242.32, result.net_value)
