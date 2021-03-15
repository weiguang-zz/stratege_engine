import logging
import os
from email import encoders
from email.header import Header
from email.mime.text import MIMEText
from smtplib import SMTP_SSL



os.environ['config.dir'] = "/Users/zhang/PycharmProjects/strategy_engine_v2/interface"
# 如果没有日志目录的话，则创建
if not os.path.exists("log"):
    os.makedirs("log")

from se.domain2.domain import send_email

from ibapi.common import BarData
from trading_calendars import get_calendar, TradingCalendar

from se.domain2.engine.engine import Engine, Scope, DataPortal
import pandas as pd

from pandas._libs.tslibs.timestamps import Timestamp

from se.domain2.account.account import LimitOrder, OrderDirection, MKTOrder, OrderStatus
from se.infras.ib import IBAccount, IBClient

from se import BeanContainer, TimeSeriesRepo
from se.domain2.time_series.time_series import TimeSeries, TimeSeriesSubscriber, TSData, HistoryDataQueryCommand

# ts_repo: TimeSeriesRepo = BeanContainer.getBean(TimeSeriesRepo)
# ts: TimeSeries = ts_repo.find_one("ibTick")
# class TestSubscriber(TimeSeriesSubscriber):
#
#     def on_data(self, data: TSData):
#         import logging
#         logging.info("data:{}".format(data.__dict__))
#
#
# ts.subscribe(TestSubscriber(), ["GME_STK_USD_SMART"])
# print("done")

# acc = IBAccount("test100", 100)
#
# acc.with_client("localhost", 7497, 98)
#
# o = LimitOrder("GSX_STK_USD_SMART", OrderDirection.BUY, 90, Timestamp.now(), 90)
# acc.place_order(o)

# print("done")


# ts_repo: TimeSeriesRepo = BeanContainer.getBean(TimeSeriesRepo)
# ts: TimeSeries = ts_repo.find_one("ibTick")
# start = Timestamp("2021-02-09 22:30:00", tz='Asia/Shanghai')
# end = Timestamp("2021-02-09 22:40:00", tz='Asia/Shanghai')
# command = HistoryDataQueryCommand(start, end, ['GSX_STK_USD_SMART'])
# from trading_calendars import get_calendar
# command.with_calendar(get_calendar("NYSE"))
# df = ts.history_data(command, from_local=False)
# print("done")

# engine = Engine()
# scope = Scope(["GSX_STK_USD_SMART"], trading_calendar=get_calendar("NYSE"))
# strategy = TestStrategy3(scope)
#
# # 回测
# start = pd.Timestamp("2019-06-06", tz='Asia/Shanghai')
# end = pd.Timestamp("2021-02-01", tz='Asia/Shanghai')
# result = engine.run_backtest(strategy, start, end, 10000, "test201", 'ibAdjustedDailyBar')
# print("done")

# engine = Engine()
# scope = Scope(["SPCE_STK_USD_SMART"], trading_calendar=get_calendar("NYSE"))
# strategy = TestStrategy2(scope)
# #
# # # 回测
# start = pd.Timestamp("2019-07-09", tz='Asia/Shanghai')
# end = pd.Timestamp("2021-02-18", tz='Asia/Shanghai')
# result = engine.run_backtest(strategy, start, end, 10000, "test210", 'ibAdjustedDailyBar')
# print("done")
# from se import config
# try:
#     # 登录
#     smtp = SMTP_SSL(config.get('email', 'host_server'))
#     smtp.set_debuglevel(0)
#     smtp.ehlo(config.get('email', 'host_server'))
#     smtp.login(config.get('email', 'username'), config.get('email', 'password'))
#
#     sender_email = config.get('email', 'sender_email')
#     receiver = config.get('email', 'receiver')
#     msg = MIMEText("'status': <OrderStatus.CREATED: 'CREATED'>", "plain", 'utf-8')
#     msg["Subject"] = Header("hahaha", 'utf-8')
#     msg["From"] = sender_email
#     msg["To"] = receiver
#     smtp.sendmail(sender_email, receiver, msg.as_string())
#     smtp.quit()
#     print("done")
# except:
#     import traceback


# from se.domain2.account.account import *
# acc_repo: AccountRepo = BeanContainer.getBean(AccountRepo)
# acc: AbstractAccount = acc_repo.find_one("test209")
# pd.Series(acc.history_net_value)
# start = pd.Timestamp("2019-02-09", tz='Asia/Shanghai')
# end = pd.Timestamp("2019-02-18", tz='Asia/Shanghai')
# command = HistoryDataQueryCommand(start, end, ['AAPL_STK_USD_ARCA'])
# c = IBClient('172.16.0.102', 7496, '98')
# c.req_history_data("ACB_STK_USD_SMART", end, duration_str='1 Y', bar_size='1 day', what_to_show='TRADES', use_rth=1,
#                    format_date=1, keep_up_to_date=False,
#                    char_options=None)

# import trading_calendars
# dp = DataPortal(False, "ibTick", subscribe_codes=['SPCE_STK_USD_SMART'])
# command = HistoryDataQueryCommand(None, None, ['SPCE_STK_USD_SMART'], window=1)
# command.with_calendar(trading_calendar=trading_calendars.get_calendar("NYSE"))
# df = dp.history_data("ibAdjustedDailyBar", command)
# print("done")


# from trading_calendars import get_calendar
#
# from se import config, BeanContainer, AccountRepo
# from se.domain2.engine.engine import Engine, Scope
# from se.infras.ib import IBAccount
# from strategies.strategy import TestStrategy2
#
# engine = Engine()
# scope = Scope(["SPCE_STK_USD_SMART"], trading_calendar=get_calendar("NYSE"))
# strategy = TestStrategy2(scope)
#
# account_name = "ib_test1"
# repo: AccountRepo = BeanContainer.getBean(AccountRepo)
# acc = repo.find_one(account_name)
# if not acc:
#     acc = IBAccount(account_name, 10000)
#
# acc.with_order_callback(strategy).with_client(config.get('ib_account', 'host'), config.getint('ib_account', 'port'),
#                                               config.getint('ib_account', 'client_id'))
# engine.run(strategy, acc)


# 构建策略
# import logging
#
# from se.domain2.account.account import AbstractAccount, MKTOrder, OrderDirection, LimitOrder
# from se.domain2.domain import send_email
# from se.domain2.engine.engine import AbstractStrategy, Engine, EventDefinition, EventDefinitionType, MarketOpen, \
#     MarketClose, Event, DataPortal
# from se.domain2.time_series.time_series import HistoryDataQueryCommand
#
#
# class TestStrategy2(AbstractStrategy):
#     """
#     该策略会在收盘的时候，检查今日收盘是否大于今日开盘，如果大于，则做多
#     会在开盘的时候，判断昨日收盘是否大于昨日开盘，如果大于，则做空
#     交易标的： ACB
#     """
#
#     def do_initialize(self, engine: Engine, data_portal: DataPortal):
#         if engine.is_backtest:
#             market_close = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketClose())
#             market_open = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketOpen())
#         else:
#             market_open = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketOpen(second_offset=5))
#             market_close = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketClose(second_offset=-30))
#         engine.register_event(market_open, self.market_open)
#         engine.register_event(market_close, self.market_close)
#         # 初始化昨日开盘价和收盘价
#         self.last_open = None
#         self.last_close = None
#         if not engine.is_backtest:
#             command = HistoryDataQueryCommand(None, None, self.scope.codes, window=1)
#             command.with_calendar(trading_calendar=self.scope.trading_calendar)
#             df = data_portal.history_data("ibAdjustedDailyBar", command)
#             if len(df) >= 1:
#                 self.last_open = df.iloc[-1]['open']
#                 self.last_close = df.iloc[-1]['close']
#                 logging.info("初始化数据成功，昨日开盘价:{}, 昨日收盘价:{}, bar的开始时间:{}"
#                              .format(self.last_open, self.last_close, df.iloc[-1]['start_time']))
#             else:
#                 raise RuntimeError("没有获取到昨日开盘价和收盘价")
#
#         if len(self.scope.codes) != 1:
#             raise RuntimeError("wrong codes")
#         self.code = self.scope.codes[0]
#
#     def market_open(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
#         dest_position = 0
#         current_position = 0
#         net_value = None
#
#         # 等待直到获取到最新的股票价格
#         current_price = self.get_recent_price_after([self.code], event.visible_time, data_portal)
#         if not current_price:
#             msg = "没有获取到当天的开盘价,code:{}".format(self.code)
#             logging.error(msg)
#             send_email("ERROR", msg)
#         else:
#             net_value = account.net_value({self.code: current_price})
#
#         if len(account.positions) > 0:
#             current_position = account.positions[self.code]
#
#         if current_price and self.last_open and self.last_close and self.last_close > self.last_open:
#             dest_position = - int(net_value / current_price)
#
#         change = dest_position - current_position
#         if change != 0:
#             direction = OrderDirection.BUY if change > 0 else OrderDirection.SELL
#             if current_price:
#                 order = LimitOrder(self.code, direction, abs(change), event.visible_time, current_price)
#             else:
#                 logging.warning("没有获取到当前价格，将会以市价单下单")
#                 order = MKTOrder(self.code, direction, abs(change), event.visible_time)
#             account.place_order(order)
#             msg = "时间:{}, 当前持仓:{}, 总市值：{}, 目标持仓:{}, 昨日开盘价:{}, 昨日收盘价:{}, 今日开盘价：{}, 订单:{}" \
#                 .format(event.visible_time, current_position, net_value, dest_position, self.last_open, self.last_close,
#                         current_price, order.__dict__)
#             logging.info("开盘下单:{}".format(msg))
#             send_email('[订单]', msg)
#         else:
#             msg = "不需要下单, 时间:{}, 当前持仓:{}, 总市值：{}, 目标持仓:{}, 昨日开盘价:{}, 昨日收盘价:{}, 今日开盘价：{}". \
#                 format(event.visible_time, current_position, net_value, dest_position, self.last_open, self.last_close,
#                        current_price)
#             logging.info(msg)
#
#         self.last_open = current_price
#
#     def market_close(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
#         dest_position = 0
#         current_position = 0
#         net_value = None
#
#         current_price = self.get_recent_price_after([self.code], event.visible_time, data_portal)
#         if not current_price:
#             msg = "没有获取到当前价格, code:{}".format(self.code)
#             logging.error(msg)
#             send_email("ERROR", msg)
#         else:
#             net_value = account.net_value({self.code: current_price})
#
#         if current_price and self.last_open and current_price > self.last_open:
#             dest_position = int(net_value / current_price)
#
#         if len(account.positions) > 0:
#             current_position = account.positions[self.code]
#
#         change = dest_position - current_position
#         if change != 0:
#             direction = OrderDirection.BUY if change > 0 else OrderDirection.SELL
#             quantity_split = None
#             if current_position != 0:
#                 quantity_split = [-current_position, change + current_position]
#             if current_price:
#                 order = LimitOrder(self.code, direction, abs(change), event.visible_time, limit_price=current_price,
#                                    quantity_split=quantity_split)
#             else:
#                 logging.warning("没有获取到当前价格，将会以市价单下单")
#                 order = MKTOrder(self.code, direction, abs(change), event.visible_time, quantity_split)
#             account.place_order(order)
#             msg = "时间:{}, 当前持仓:{}, 总市值：{}, 目标持仓:{}, 昨日收盘价:{}, 今日收盘价:{}, 订单:{}".format(event.visible_time,
#                                                                                       current_position,
#                                                                                       net_value, dest_position,
#                                                                                       self.last_close,
#                                                                                       current_price, order.__dict__)
#             logging.info("收盘下单:{}".format(msg))
#             send_email("[订单]", msg)
#         else:
#             logging.info("不需要下单, 时间:{}, 当前持仓:{}, 总市值：{}, 目标持仓:{}, 今日开盘价:{}, 今日收盘价:{}".
#                          format(event.visible_time,
#                                 current_position,
#                                 net_value, dest_position,
#                                 self.last_open,
#                                 current_price))
#
#         self.last_close = current_price
#
#     def order_status_change(self, order, account):
#         logging.info("订单状态变更, 订单状态:{}，账户持仓:{}, 账户现金:{}".
#                      format(order.__dict__, account.positions, account.cash))
#         msg = {"positions": account.positions, "cash": account.cash, "order": order.__dict__}
#         send_email("【订单】成交", str(msg))

# 回测
# engine = Engine()
# scope = Scope(["ACB_STK_USD_ISLAND"], trading_calendar=get_calendar("NYSE"))
# strategy = TestStrategy2(scope)
#
# start = pd.Timestamp("2019-02-18", tz='Asia/Shanghai')
# end = pd.Timestamp("2021-02-18", tz='Asia/Shanghai')
# result = engine.run_backtest(strategy, start, end, 10000, "test215", 'ibAdjustedDailyBar')
# print("done")


# from se import config
# cli = IBClient.find_client(config.get('ib_data', 'host'), config.getint('ib_data', 'port'),
#                                               config.getint('ib_data', 'client_id'))
# if not cli:
#     cli: IBClient = IBClient(config.get('ib_data', 'host'), config.getint('ib_data', 'port'),
#                                               config.getint('ib_data', 'client_id'))
# code = "AAPL_STK_USD_SMART"
# cli.query_contract(cli.code_to_contract(code))
# print("done")

# from se.domain2.domain import send_email
# send_email("haha", "sssss")

# from se import config
# from ibapi.order import Order as IBOrder
#
# account_name = "test"
# acc = IBAccount(account_name, 10000)
# acc.with_client(config.get('ib_account', 'host'), config.getint('ib_account', 'port'),
#                 config.getint('ib_account', 'client_id'))
# code = 'GSX_STK_USD_SMART'
# contract = acc.cli.code_to_contract(code)
# ib_order: IBOrder = IBOrder()
# ib_order.orderType = "LMT"
# ib_order.totalQuantity = order.quantity
# # 价格调整到两位小数
# ib_order.lmtPrice = round(order.limit_price, 2)
# ib_order.action = 'BUY' if order.direction == OrderDirection.BUY else 'SELL'
# ib_order.outsideRth = True
# acc.cli.placeOrder()
# from se import config
# print('ss:{}'.format(config.__dict__))

# while True:
#     send_email("test", 'test')
#     import time
#     time.sleep(200)

from trading_calendars import get_calendar
calendar: TradingCalendar = get_calendar("NYSE")
o = calendar.next_open(Timestamp.now(tz='Asia/Shanghai'))

print('done')