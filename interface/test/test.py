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

from interface.test.test_strategy import TestStrategy3


from ibapi.common import BarData
from trading_calendars import get_calendar

from se.domain2.engine.engine import Engine, Scope, DataPortal
import pandas as pd

from pandas._libs.tslibs.timestamps import Timestamp

from se.domain2.account.account import LimitOrder, OrderDirection, MKTOrder
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


from trading_calendars import get_calendar

from se import config, BeanContainer, AccountRepo
from se.domain2.engine.engine import Engine, Scope
from se.infras.ib import IBAccount
from strategies.strategy import TestStrategy2

engine = Engine()
scope = Scope(["SPCE_STK_USD_SMART"], trading_calendar=get_calendar("NYSE"))
strategy = TestStrategy2(scope)

account_name = "ib_test1"
repo: AccountRepo = BeanContainer.getBean(AccountRepo)
acc = repo.find_one(account_name)
if not acc:
    acc = IBAccount(account_name, 10000)

acc.with_order_callback(strategy).with_client(config.get('ib_account', 'host'), config.getint('ib_account', 'port'),
                                              config.getint('ib_account', 'client_id'))
engine.run(strategy, acc)
