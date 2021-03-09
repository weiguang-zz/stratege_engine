import logging
import os
from email import encoders
from email.header import Header
from email.mime.text import MIMEText
from smtplib import SMTP_SSL

from ibapi.tag_value import TagValue
from pandas._libs.tslibs.timestamps import Timestamp



os.environ['config.dir'] = "/Users/zhang/PycharmProjects/strategy_engine_v2/interface"
# 如果没有日志目录的话，则创建
if not os.path.exists("log"):
    os.makedirs("log")

from se.domain2.engine.engine import AbstractStrategy, Engine, DataPortal, Scope
from se.domain2.account.account import MKTOrder, Order, OrderDirection, LimitOrder
from se import config
from ibapi.order import Order as IBOrder
from se.infras.ib import IBAccount

class TestStrategy(AbstractStrategy):

    def do_order_status_change(self, order, account):
        pass

    def do_initialize(self, engine: Engine, data_portal: DataPortal):
        pass


account_name = "test"
acc = IBAccount(account_name, 10000)
acc.with_client(config.get('ib_account', 'host'), config.getint('ib_account', 'port'),
                config.getint('ib_account', 'client_id'))

acc.with_order_callback(TestStrategy(None))
code = '700_STK_HKD_SEHK'
contract = acc.cli.code_to_contract(code)
ib_order: IBOrder = IBOrder()
ib_order.orderType = "MKT"
ib_order.totalQuantity = 100
# ib_order.lmtPrice = 85
ib_order.action = 'SELL'
# ib_order.outsideRth = True
# ib_order.tif = "GTD"
# ib_order.goodAfterTime = '18:45:00'
# ib_order.goodTillDate = '19:56:00'
# ib_order.orderType = "LIMIT"
# ib_order.lmtPrice = 100
# ib_order.totalQuantity = 100
# ib_order.action = 'BUY'
# ib_order.outsideRth = True
# # 设置自适应算法
# ib_order.algoStrategy = 'Adaptive'
# ib_order.algoParams = [TagValue("adaptivePriority", 'Normal')]

order: Order = LimitOrder(code, OrderDirection.BUY, quantity=1000, place_time=Timestamp.now(), limit_price=644)

# acc.cli.cli.cancelOrder(34)
# acc.cli.placeOrder(acc.cli.next_valid_id(), contract, ib_order)
acc.place_order(order)
print("done")