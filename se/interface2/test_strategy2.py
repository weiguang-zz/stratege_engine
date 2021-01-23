import logging
import time
from configparser import ConfigParser

from trading_calendars import get_calendar

from se.domain2.account.account import AbstractAccount, MKTOrder, OrderDirection
from se.domain2.domain import send_email
from se.domain2.engine.engine import AbstractStrategy, Engine, Scope, EventDefinition, EventDefinitionType, MarketOpen, \
    MarketClose, Event, DataPortal
from se.infras.ib import IBAccount


class TestStrategy2(AbstractStrategy):
    """
    该策略会在收盘的时候，检查当天的涨跌幅，如果是阳柱，则以市价买入，并持有到下一个开盘卖出
    交易标的： SPCE
    """

    def initialize(self, engine: Engine):
        market_open = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketOpen())
        market_close = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketClose())
        engine.register_event(market_open, self.market_open)
        engine.register_event(market_close, self.market_close)

    def market_open(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
        if len(account.positions) > 0:
            for code in account.positions.keys():
                order = MKTOrder(code, direction=OrderDirection.SELL, quantity=account.positions[code],
                                 place_time=event.visible_time)
                account.place_order(order)
                logging.info("开盘平仓, 订单:{}".format(order.__dict__))
                msg = {"positions": account.positions, "cash": account.cash, "order": order.__dict__}
                send_email("【订单】开盘平仓", str(msg))
        # 记录当天的开盘价
        code = self.scope.codes[0]
        # 等待直到获取到最新的股票价格
        while True:
            cp = data_portal.current_price(self.scope.codes, event.visible_time)
            if cp[code].time >= event.visible_time:
                self.open_price = cp[code].price
                break
            logging.info("没有获取到最新的价格，将会重试")
            time.sleep(1)

    def market_close(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
        if not len(self.scope.codes) == 1:
            raise RuntimeError("wrong scope")
        if len(account.positions) > 0:
            raise RuntimeError("wrong positions")
        if not self.open_price:
            logging.warning("没有设置开盘价, 不操作")
            return
        code = self.scope.codes[0]
        cp = data_portal.current_price([code], event.visible_time)
        if cp[code].price > self.open_price:
            buy_quantity = int(account.cash / cp[code].price)
            order = MKTOrder(code, direction=OrderDirection.BUY, quantity=buy_quantity, place_time=event.visible_time)
            account.place_order(order)
            logging.info("当天价格上升，下单买入, 开盘价：{}, 收盘价:{}, 订单：{}".
                         format(self.open_price, cp[code].price, order.__dict__))
            msg = {"positions": account.positions, "cash": account.cash, "order": order.__dict__}
            send_email("【订单】收盘买入", str(msg))
        else:
            logging.info("当天价格下跌，不开仓 开盘价:{}, 收盘价:{}".format(self.open_price, cp[code].price))

        # 清理开盘价
        self.open_price = None

    def order_status_change(self, order, account):
        logging.info("订单状态变更, 订单状态:{}，账户持仓:{}, 账户现金:{}".
                     format(order.__dict__, account.positions, account.cash))
        msg = {"positions": account.positions, "cash": account.cash, "order": order.__dict__}
        send_email("【订单】成交", str(msg))



engine = Engine()
scope = Scope(["SPCE_STK_USD_SMART"], trading_calendar=get_calendar("NYSE"))
strategy = TestStrategy2(scope)

# 初始化配置
config_file_name = 'config.ini'
config = ConfigParser()
config.read(config_file_name)

# 回测
# start = pd.Timestamp("2020-01-01", tz='Asia/Shanghai')
# end = pd.Timestamp("2020-12-01", tz='Asia/Shanghai')
# result = engine.run_backtest(strategy, start, end, 10000, "test22")
# print("done")

# 实盘测试
# acc = IBAccount("ib_test1", 10000)

# acc_repo: AccountRepo = BeanContainer.getBean(AccountRepo)
# acc = acc_repo.find_one("ib_test1")
#
# acc.with_order_callback(strategy).with_client(config.get('ib', 'host'), config.getint('ib', 'port'),
#                                               config.getint('ib', 'client_id'))
#
#
# def mocked_event_generator(event_definition: EventDefinition):
#     if isinstance(event_definition.time_rule, MarketOpen):
#         t = Timestamp("2021-01-21 22:30:00", tz='Asia/Shanghai')
#         return [Event(event_definition, visible_time=t, data={})]
#
#     elif isinstance(event_definition.time_rule, MarketClose):
#         if event_definition.time_rule.offset == 0:
#             t = Timestamp("2021-01-22 05:00:00", tz='Asia/Shanghai')
#             return [Event(event_definition, visible_time=t, data={})]
#         elif event_definition.time_rule.offset == 30:
#             t = Timestamp("2021-01-22 05:30:00", tz='Asia/Shanghai')
#             return [Event(event_definition, visible_time=t, data={})]
#
#
# mocked_current_prices = {
#     Timestamp("2021-01-21 22:30:00", tz='Asia/Shanghai'): {"SPCE_STK_USD_SMART": 30},
#     Timestamp("2021-01-22 05:00:00", tz='Asia/Shanghai'): {"SPCE_STK_USD_SMART": 31},
#     Timestamp("2021-01-22 05:30:00", tz='Asia/Shanghai'): {"SPCE_STK_USD_SMART": 31},
#
# }
#
# engine.run(strategy, acc, is_realtime_test=True, mocked_events_generator=mocked_event_generator,
#            mocked_current_prices=mocked_current_prices)

# 实盘

acc = IBAccount("ib_real1", 10000)
acc.with_order_callback(strategy).with_client(config.get('ib', 'host'), config.getint('ib', 'port'),
                                              config.getint('ib', 'client_id'))
engine.run(strategy, acc)
