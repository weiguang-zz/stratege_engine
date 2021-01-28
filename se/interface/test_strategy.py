import logging
import time

from pandas._libs.tslibs.timedeltas import Timedelta
from pandas._libs.tslibs.timestamps import Timestamp

from se.domain2.account.account import Order, AbstractAccount, MKTOrder, LimitOrder, OrderDirection, OrderStatus
from se.domain2.engine.engine import AbstractStrategy, Event, DataPortal, MarketOpen, \
    MarketClose, Engine, Scope, EventDefinition, EventDefinitionType


class TestStrategy1(AbstractStrategy):

    def do_initialize(self, engine: Engine):

        engine.register_event(EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketOpen()),
                              self.market_open)
        engine.register_event(EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketClose(offset=-1)),
                              self.market_close)

    def market_open(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
        if len(account.positions) > 0:
            raise RuntimeError("错误的账户状态")
        # 检查最新价格的时间是否是开盘之后的价格
        open_time = event.visible_time
        code = self.scope.codes[0]
        while True:
            cp = data_portal.current_price([code], event.visible_time)[code]
            if cp.time >= open_time:
                break
            logging.info("没有获取到最新的价格，将会重试, 获取到的价格是:{}, 事件时间是:{}".format(cp, event.visible_time))
            time.sleep(1)
        quantity = int(account.cash * 0.5 / cp.price)
        buy_order = MKTOrder(code, OrderDirection.BUY, quantity, event.visible_time)
        account.place_order(buy_order)
        self.base_price = cp.price

    def market_close(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
        account.cancel_all_open_orders()
        for code in account.positions.keys():
            sell_order = MKTOrder(code, OrderDirection.SELL, account.positions[code], event.visible_time)
            account.place_order(sell_order)

    def order_status_change(self, order: Order, account: AbstractAccount):
        if order.status != OrderStatus.FILLED:
            return
        current_time = order.filled_end_time
        if current_time >= (self.scope.trading_calendar.next_close(current_time) - Timedelta(minutes=1)):
            # 收盘前一分钟不下单
            return
        # 取消当前所有的订单
        account.cancel_all_open_orders()

        # 挂上下两个网格的交易订单
        k = round((order.filled_avg_price - self.base_price) / self.base_price / self.p)
        hold_quantity = 0 if self.code not in account.positions else account.positions[self.code]
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
        self.code = scope.codes[0]

# from trading_calendars import get_calendar

# scope = Scope(codes=['CCL_STK_USD_SMART'], trading_calendar=get_calendar("NYSE"))
# start = Timestamp("2020-01-01", tz='Asia/Shanghai')
# end = Timestamp("2020-01-30", tz='Asia/Shanghai')
# Engine().run_backtest(TestStrategy1(scope), start, end, 10000, "test3")

if __name__ == "__main__":
    # 计算策略的回报序列
    from trading_calendars import get_calendar
    import pandas as pd
    calendar = get_calendar("NYSE")
    engine = Engine()
    scope = Scope(['CCL_STK_USD_SMART'], calendar)
    st = TestStrategy1(scope, n=5, p=0.01)
    start = pd.Timestamp("2020-01-01", tz='Asia/Shanghai')
    end = pd.Timestamp("2020-01-30", tz='Asia/Shanghai')
    result = engine.run_backtest(st, start, end, 10000, "test16")
    print("done")