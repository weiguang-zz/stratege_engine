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



from ibapi.common import BarData
from trading_calendars import get_calendar

from se.domain2.engine.engine import Engine, Scope
from strategies.strategy import TestStrategy3, TestStrategy2
import pandas as pd

from pandas._libs.tslibs.timestamps import Timestamp

from se.domain2.account.account import LimitOrder, OrderDirection, MKTOrder
from se.infras.ib import IBAccount



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


ts_repo: TimeSeriesRepo = BeanContainer.getBean(TimeSeriesRepo)
ts: TimeSeries = ts_repo.find_one("ibTick")
start = Timestamp("2021-02-09 22:30:00", tz='Asia/Shanghai')
end = Timestamp("2021-02-09 22:40:00", tz='Asia/Shanghai')
command = HistoryDataQueryCommand(start, end, ['GSX_STK_USD_SMART'])
from trading_calendars import get_calendar
command.with_calendar(get_calendar("NYSE"))
df = ts.history_data(command, from_local=False)
print("done")

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
#
# # 回测
# start = pd.Timestamp("2018-01-01", tz='Asia/Shanghai')
# end = pd.Timestamp("2021-02-01", tz='Asia/Shanghai')
# result = engine.run_backtest(strategy, start, end, 10000, "test203", 'ibAdjustedDailyBar')
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