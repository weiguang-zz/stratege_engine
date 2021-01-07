import logging
import os
from typing import *

from pandas._libs.tslibs.timedeltas import Timedelta
from pandas._libs.tslibs.timestamps import Timestamp
from trading_calendars import get_calendar

from se.domain.account import AbstractAccount, Order, OrderType, OrderDirection, OrderFilledData, IBAccount
from se.domain.data_portal import DataPortal, HistoryDataLoader, TickCurrentPriceLoader
from se.domain.engine import AbstractStrategy, StrategyEngine
from se.domain.event_producer import Event, EventType, EventProducer, TimeEventProducer, DateRules, TimeRules, \
    TimeEventCondition


class TestStrategy1(AbstractStrategy):
    """
    没有选股和择时的网格交易
    每个交易日的开盘建仓50%， 设置10个网格，每个网格的大小为建仓价格的一定百分比P
    """

    def on_event(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
        if event.event_type == EventType.TIME and event.sub_type == 'market_open':
            if len(account.get_positions()) > 0:
                raise RuntimeError("错误的账户状态")
            # 检查最新价格的时间是否是开盘之后的价格
            open_time = self.trading_calendar.previous_open(event.visible_time+Timedelta(minutes=1)) - Timedelta(minutes=1)
            while True:
                cp = data_portal.current_price([self.code])[self.code]
                if cp.time >= open_time:
                    break
                logging.info("没有获取到最新的价格，将会重试, 获取到的价格是:{}, 事件时间是:{}".format(cp, event.visible_time))
                time.sleep(1)
            quantity = int(account.cash * 0.5 / cp.price)
            buy_order = Order(self.code, quantity, order_type=OrderType.MKT, direction=OrderDirection.BUY,
                              time=event.visible_time)
            account.place_order(buy_order)
            self.base_price = cp.price

        if event.event_type == EventType.TIME and event.sub_type == 'market_close':
            account.cancel_all_open_orders()
            for position in account.get_positions().values():
                sell_order = Order(position.code, position.quantity, order_type=OrderType.MKT,
                                   direction=OrderDirection.SELL, time=event.visible_time)
                account.place_order(sell_order)

        if event.event_type == EventType.ACCOUNT and event.sub_type == "order_filled":
            if event.visible_time >= (self.trading_calendar.next_close(event.visible_time) - Timedelta(minutes=1)):
                # 收盘前一分钟不下单
                return

            # 挂上下两个网格的交易订单
            data: OrderFilledData = event.data
            k = round((data.price - self.base_price) / self.base_price / self.p)
            hold_quantity = 0 if self.code not in account.get_positions() else account.get_positions()[self.code].quantity
            # 上一个格子的卖单
            if (k + 1) <= self.n:
                up_percentage = 0.5 - (k + 1) * (0.5 / self.n)
                up_price = self.base_price + self.base_price * self.p * (k + 1)
                up_net_val = hold_quantity * up_price + account.cash
                dest_quantity = int(up_net_val * up_percentage / up_price)
                sell_order = Order(self.code, hold_quantity - dest_quantity, OrderType.LIMIT,
                                   direction=OrderDirection.SELL, time=event.visible_time, limit_price=up_price)
                account.place_order(sell_order)
            # 下一个格子的买单
            if (k - 1) >= -self.n:
                down_percentage = 0.5 - (k - 1) * (0.5 / self.n)
                down_price = self.base_price + self.base_price * self.p * (k - 1)
                down_net_val = hold_quantity * down_price + account.cash
                dest_quantity = int(down_net_val * down_percentage / down_price)
                buy_order = Order(self.code, dest_quantity - hold_quantity, OrderType.LIMIT,
                                  direction=OrderDirection.BUY, time=event.visible_time, limit_price=down_price)
                account.place_order(buy_order)

    def __init__(self, code, n=5, p=0.02):
        """

        :param code: 资产代号
        :param n: 单边网格数量
        :param p: 每个网格大小
        """
        calendar = get_calendar("NYSE")
        # 嘉年华
        # self.code = "CCL_STK_USD_SMART"
        self.code = code
        self.n = n
        self.p = p
        conditions = [TimeEventCondition(DateRules.every_day(),
                                         TimeRules.market_open(calendar=calendar),
                                         name="market_open"),
                      TimeEventCondition(DateRules.every_day(),
                                         TimeRules.market_close(calendar=calendar, offset=-1),
                                         name="market_close")]
        tep = TimeEventProducer(conditions)
        eps: List[EventProducer] = [tep]

        super(TestStrategy1, self).__init__(eps, calendar)


def SetupLogger():
    if not os.path.exists("log"):
        os.makedirs("log")

    time.strftime("pyibapi.%Y%m%d_%H%M%S.log")

    recfmt = '(%(threadName)s) %(asctime)s.%(msecs)03d %(levelname)s %(filename)s:%(lineno)d %(message)s'

    timefmt = '%y%m%d_%H:%M:%S'

    # logging.basicConfig( level=logging.DEBUG,
    #                    format=recfmt, datefmt=timefmt)
    logging.basicConfig(filename=time.strftime("log/pyibapi.%y%m%d_%H%M%S.log"),
                        filemode="w",
                        level=logging.INFO,
                        format=recfmt, datefmt=timefmt)
    logger = logging.getLogger()
    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter(fmt=recfmt, datefmt=timefmt))
    console.setLevel(logging.DEBUG)
    logger.addHandler(console)


if __name__ == "__main__":
    SetupLogger()

    strategy = TestStrategy1(code="CCL_STK_USD_SMART", n=5, p=0.01)
    current_price_loader = TickCurrentPriceLoader(tick_loader=HistoryDataLoader(data_provider_name="ib",
                                                                                ts_type_name='tick'),
                                                  calendar=strategy.trading_calendar, is_realtime=True)
    account = IBAccount("192.168.0.221", 4002, 8, 30000)
    engine = StrategyEngine(None)
    engine.run(strategy, account, current_price_loader)
    import time
    time.sleep(5)
    engine.on_event(Event(EventType.TIME, sub_type="market_open", visible_time=Timestamp.now(tz='Asia/Shanghai'),
                          data={}))
    # time.sleep(10)
    # engine.on_event(Event(EventType.TIME, sub_type="market_close", visible_time=Timestamp.now(tz='Asia/Shanghai'),
    #                       data={}))
