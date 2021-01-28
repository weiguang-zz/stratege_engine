from configparser import ConfigParser

from trading_calendars import get_calendar

from se.domain2.engine.engine import Engine, Scope
from se.infras.ib import IBAccount
from se.strategies.strategy import TestStrategy2

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
