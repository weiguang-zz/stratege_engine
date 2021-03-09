from pandas import DataFrame, Series
from pandas._libs.tslibs.timedeltas import Timedelta
from pandas._libs.tslibs.timestamps import Timestamp

from se.domain2.account.account import AbstractAccount, LimitOrder, OrderDirection
from se.domain2.engine.engine import AbstractStrategy, Engine, DataPortal, EventDefinition, EventDataType, \
    EventDefinitionType, Event, MarketOpen
from se.domain2.time_series.time_series import Tick
import logging


class TestStrategy(AbstractStrategy):
    """
    测试的策略，当股价在一定时间内上涨超过一定阀值的话，则买入，在股价在一定时间内下跌超过某个阀值，则卖出
    该策略不支持回测
    """

    def do_order_status_change(self, order, account):
        pass

    def do_initialize(self, engine: Engine, data_portal: DataPortal):
        if engine.is_backtest:
            raise RuntimeError("不支持回测")
        engine.register_event(EventDefinition(ed_type=EventDefinitionType.DATA, ts_type_name='ibTick',
                                              event_data_type=EventDataType.TICK), self.on_tick)
        engine.register_event(event_definition=EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketOpen()),
                              callback=self.market_open)
        engine.register_event(
            event_definition=EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketOpen()),
            callback=self.market_close)

        self.daily_ticks: DataFrame = DataFrame()
        self.time_span = Timedelta(minutes=1)
        self.threshold = 0.005
        self.code = self.scope.codes[0]
        self.market_is_open = self.scope.trading_calendar.is_open_on_minute(Timestamp.now(tz='Asia/Shanghai'))

    def on_tick(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
        if not self.market_is_open:
            return
        if not isinstance(event.data, Tick):
            raise RuntimeError("wrong data")
        data = DataFrame([{'visible_time': event.data.visible_time, 'price': event.data.price}]).set_index('visible_time')
        self.daily_ticks.append(data)
        now = Timestamp.now(tz='Asia/Shanghai')
        start = now - self.time_span
        check_df = self.daily_ticks[start: now]
        this_price = event.data.price
        if len(account.get_open_orders()) <= 0:
            if len(account.positions) > 0:
                max_price = check_df['price'].max()
                change = abs((this_price - max_price) / max_price)
                if change > self.threshold:
                    reason = '当前价格:{}, 时间范围：{}的最高价为：{}, 当前持仓:{}'.\
                        format(this_price, self.time_span, max_price, account.positions)
                    order = LimitOrder(self.code, OrderDirection.SELL, quantity=abs(account.positions[self.code]),
                                       place_time=event.visible_time, limit_price=this_price)
                    order.with_reason(reason)
                    account.place_order(order)
                    self.ensure_order_filled(account, data_portal, order, period=10, retry_count=2)
                else:
                    logging.info("当前价格:{}, 时间范围：{}的最高价为：{}, 变动为:{}".
                                 format(this_price, self.time_span, max_price, change))
            else:
                lowest_price = check_df['price'].min()
                change = abs((this_price - lowest_price) / lowest_price)
                if change > self.threshold:
                    reason = '当前价格:{}, 时间范围：{}的最低价为：{}, 当前持仓:{}'.\
                        format(this_price, self.time_span, lowest_price, account.positions)
                    quantity = int(account.cash / this_price)
                    order = LimitOrder(self.code, OrderDirection.BUY, quantity=quantity,
                                       place_time=event.visible_time, limit_price=this_price)
                    order.with_reason(reason)
                    account.place_order(order)
                    self.ensure_order_filled(account, data_portal, order, period=10, retry_count=2)
                else:
                    logging.info("当前价格:{}, 时间范围：{}的最低价为：{}, 变动为:{}".
                                 format(this_price, self.time_span, lowest_price, change))

    def market_open(self):
        logging.info("market is open")
        self.daily_ticks = DataFrame()
        self.market_is_open = True

    def market_close(self):
        logging.info('market close')
        self.daily_ticks = DataFrame()
        self.market_is_open = False