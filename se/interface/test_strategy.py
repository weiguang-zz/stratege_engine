import logging
import time

from pandas._libs.tslibs.timedeltas import Timedelta
from pandas._libs.tslibs.timestamps import Timestamp

from se.domain2.account.account import Order, AbstractAccount, MKTOrder, LimitOrder, OrderDirection
from se.domain2.engine.engine import AbstractStrategy, Event, DataPortal, TimeEventDefinition, EveryDay, MarketOpen, \
    MarketClose, Context, Engine, Scope


class TestStrategy1(AbstractStrategy):

    def initialize(self, engine: Engine):

        engine.register_event(TimeEventDefinition("market_open", date_rule=EveryDay(),
                                                   time_rule=MarketOpen(calendar=context.scope.trading_calendar)),
                               self.market_open)
        engine.register_event(TimeEventDefinition("market_close", date_rule=EveryDay(),
                                                   time_rule=MarketClose(calendar=context.scope.trading_calendar)),
                               self.market_close)

    def market_open(self, event: Event, account: AbstractAccount, data_portal: DataPortal, context: Context):
        if len(account.positions) > 0:
            raise RuntimeError("错误的账户状态")
        # 检查最新价格的时间是否是开盘之后的价格
        open_time = event.visible_time
        code = context.scope.codes[0]
        while True:
            cp = data_portal.current_price([code])[code]
            if cp.time >= open_time:
                break
            logging.info("没有获取到最新的价格，将会重试, 获取到的价格是:{}, 事件时间是:{}".format(cp, event.visible_time))
            time.sleep(1)
        quantity = int(account.cash * 0.5 / cp.price)
        buy_order = MKTOrder(code, quantity, event.visible_time)
        account.place_order(buy_order)
        context.base_price = cp.price

    def market_close(self, event: Event, account: AbstractAccount, data_portal: DataPortal, context: Context):
        account.cancel_all_open_orders()
        for code in account.positions.keys():
            sell_order = MKTOrder(code, account.positions[code], event.visible_time)
            account.place_order(sell_order)

    def order_status(self, order: Order, account: AbstractAccount, context: Context):

        current_time = order.filled_end_time
        if current_time >= (self.trading_calendar.next_close(current_time) - Timedelta(minutes=1)):
            # 收盘前一分钟不下单
            return

        # 挂上下两个网格的交易订单
        k = round((order.filled_avg_price - self.base_price) / self.base_price / self.p)
        hold_quantity = 0 if context.code not in account.positions else account.get_positions()[self.code].quantity
        # 上一个格子的卖单
        if (k + 1) <= self.n:
            up_percentage = 0.5 - (k + 1) * (0.5 / self.n)
            up_price = self.base_price + self.base_price * self.p * (k + 1)
            up_net_val = hold_quantity * up_price + account.cash
            dest_quantity = int(up_net_val * up_percentage / up_price)
            sell_order = LimitOrder(self.code, OrderDirection.SELL, hold_quantity - dest_quantity,
                                    current_time, up_price)
            account.place_order(sell_order)
        # 下一个格子的买单
        if (k - 1) >= -self.n:
            down_percentage = 0.5 - (k - 1) * (0.5 / self.n)
            down_price = self.base_price + self.base_price * self.p * (k - 1)
            down_net_val = hold_quantity * down_price + account.cash
            dest_quantity = int(down_net_val * down_percentage / down_price)
            buy_order = LimitOrder(self.code, OrderDirection.BUY, dest_quantity - hold_quantity, current_time,
                                   down_price)
            account.place_order(buy_order)

    def __init__(self, scope: Scope, n=5, p=0.02):
        super().__init__(scope)
        self.n = n
        self.p = p

from trading_calendars import get_calendar
scope = Scope(codes=['CCL_STK_USD_SMART'], trading_calendar=get_calendar("NYSE"))
start = Timestamp("2020-01-01", tz='Asia/Shanghai')
end = Timestamp("2020-01-01", tz='Asia/Shanghai')
Engine().run_backtest(TestStrategy1(), scope, start, end, 10000, "test1")
