import logging
import time
from configparser import ConfigParser

from pandas._libs.tslibs.timestamps import Timestamp
from trading_calendars import get_calendar

from se.domain2.account.account import AbstractAccount, MKTOrder, OrderDirection, AccountRepo, LimitOrder
from se.domain2.domain import send_email, BeanContainer
from se.domain2.engine.engine import AbstractStrategy, Engine, Scope, EventDefinition, EventDefinitionType, MarketOpen, \
    MarketClose, Event, DataPortal
import se.infras
from se.infras.ib import IBAccount
import pandas as pd


class TestStrategy3(AbstractStrategy):
    """
    在每天收盘的时候， 如果有持仓，则平仓，并且以收盘价卖空
    在每天开盘的时候，如果有持仓(一定是空仓)，则平仓， 并且判断昨天收盘到今天开盘的涨幅是否小于0.025， 若是，则已开盘价买入
    交易标的： GSX
    """

    def initialize(self, engine: Engine):
        market_open = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketOpen())
        if engine.is_backtest:
            market_close = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketClose())
        else:
            # 实盘的时候，在收盘前5s的时候提交订单
            market_close = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketClose(second_offset=-5))
        engine.register_event(market_open, self.market_open)
        engine.register_event(market_close, self.market_close)
        self.open_price = None
        self.last_close_price = None
        self.is_backtest = engine.is_backtest

    def market_open(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
        self.open_price = None
        code = self.scope.codes[0]
        buy_amount = 0
        if self.last_close_price:
            # 获取开盘价格
            if self.is_backtest:
                # 注意这里可能会有误差，因为有些股票没有开盘的第一分钟的bar数据，因为成交量太低
                self.open_price = data_portal.current_price([code], event.visible_time)[code].price
            else:
                while True:
                    cp = data_portal.current_price([code], event.visible_time)
                    if cp[code].time >= event.visible_time:
                        self.open_price = cp[code].price
                        break
                    logging.info("没有获取到最新的价格，将会重试")
                    time.sleep(1)
            if ((self.open_price - self.last_close_price) / self.last_close_price) <= 0.025:
                net_value = account.net_value({code: self.open_price})
                buy_amount += int(net_value / self.open_price)

        if len(account.positions) > 0:
            if len(account.positions) > 1:
                raise RuntimeError("非法的持仓")
            buy_amount += -account.positions[code]
        if buy_amount > 0:
            order = MKTOrder(code, direction=OrderDirection.BUY, quantity=buy_amount,
                             place_time=event.visible_time)
            account.place_order(order)
            msg = "开盘买入, 昨收:{}, 开盘价:{}, 当前持仓:{}， 订单:{}".\
                format(self.last_close_price, self.open_price, account.positions, order.__dict__)
            logging.info(msg)
            send_email("【订单】开盘下单", msg)

    def market_close(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
        self.last_close_price = None
        code = self.scope.codes[0]
        sell_amount = 0
        current_price = data_portal.current_price([code], event.visible_time)[code].price
        self.last_close_price = current_price
        if len(account.positions) > 0:
            if len(account.positions) > 1:
                raise RuntimeError("非法的持仓")
            sell_amount += account.positions[code]

        net_value = account.net_value({code: current_price})
        sell_amount += int(net_value / current_price)

        if sell_amount > 0:
            order = LimitOrder(code, direction=OrderDirection.SELL, quantity=sell_amount,
                               place_time=event.visible_time,
                               limit_price=current_price)

            account.place_order(order)
            msg = "收盘卖空, 当前价格:{}, 当前持仓:{}, 订单:{}".format(current_price, account.positions, order.__dict__)
            logging.info(msg)
            send_email("【订单】收盘卖空", msg)

    def order_status_change(self, order, account):
        logging.info("订单状态变更, 订单状态:{}，账户持仓:{}, 账户现金:{}".
                     format(order.__dict__, account.positions, account.cash))
        msg = {"positions": account.positions, "cash": account.cash, "order": order.__dict__}
        send_email("【订单】成交", str(msg))

engine = Engine()
scope = Scope(["GSX_STK_USD_SMART"], trading_calendar=get_calendar("NYSE"))
strategy = TestStrategy3(scope)

# 初始化配置
config_file_name = 'config.ini'
config = ConfigParser()
config.read(config_file_name)

# 回测
# start = pd.Timestamp("2019-06-10", tz='Asia/Shanghai')
# end = pd.Timestamp("2020-12-01", tz='Asia/Shanghai')
# result = engine.run_backtest(strategy, start, end, 10000, "test31")
# print("done")

# 实盘测试
acc = IBAccount("ib_test1", 10000)
#
# # acc_repo: AccountRepo = BeanContainer.getBean(AccountRepo)
# # acc = acc_repo.find_one("ib_test1")
# #
acc.with_order_callback(strategy).with_client(config.get('ib', 'host'), config.getint('ib', 'port'),
                                              config.getint("ib", "client_id"))
#
#
def mocked_event_generator(event_definition: EventDefinition):
    if isinstance(event_definition.time_rule, MarketOpen):
        return [Event(event_definition, visible_time=Timestamp("2021-01-21 22:30:00", tz='Asia/Shanghai'), data={}),
                Event(event_definition, visible_time=Timestamp("2021-01-22 22:30:00", tz='Asia/Shanghai'), data={}),
                ]

    elif isinstance(event_definition.time_rule, MarketClose):
        if event_definition.time_rule.minute_offset == 0:
            t = Timestamp("2021-01-22 05:00:00", tz='Asia/Shanghai')
            return [Event(event_definition, visible_time=t, data={})]
        elif event_definition.time_rule.minute_offset == 30:
            t = Timestamp("2021-01-22 05:30:00", tz='Asia/Shanghai')
            return [Event(event_definition, visible_time=t, data={})]
#
#
mocked_current_prices = {
    Timestamp("2021-01-21 22:30:00", tz='Asia/Shanghai'): {"GSX_STK_USD_SMART": 108},
    Timestamp("2021-01-22 05:00:00", tz='Asia/Shanghai'): {"GSX_STK_USD_SMART": 109},
    Timestamp("2021-01-22 05:30:00", tz='Asia/Shanghai'): {"GSX_STK_USD_SMART": 109},
    Timestamp("2021-01-22 22:30:00", tz='Asia/Shanghai'): {"GSX_STK_USD_SMART": 108},

}
#
engine.run(strategy, acc, is_realtime_test=True, mocked_events_generator=mocked_event_generator,
           mocked_current_prices=mocked_current_prices)

# 实盘

# acc = IBAccount("ib_real1", 10000)
# acc.with_order_callback(strategy).with_client(config.get('ib', 'host'), config.getint('ib', 'port'),
#                                               config.getint('ib', 'client_id'))
# engine.run(strategy, acc)
