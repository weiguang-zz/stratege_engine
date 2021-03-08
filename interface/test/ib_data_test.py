import logging
import os
from email import encoders
from email.header import Header
from email.mime.text import MIMEText
from smtplib import SMTP_SSL

from ibapi.contract import Contract
from ibapi.tag_value import TagValue

os.environ['config.dir'] = "/Users/zhang/PycharmProjects/strategy_engine_v2/interface"
# 如果没有日志目录的话，则创建
if not os.path.exists("log"):
    os.makedirs("log")

from se import config
from ibapi.order import Order as IBOrder
from se.infras.ib import IBAccount, IBClient, Request

client: IBClient = IBClient.find_client(config.get('ib_data', 'host'), config.getint('ib_data', 'port'),
                                        config.getint('ib_data', 'client_id'))
if not client:
    client: IBClient = IBClient(config.get('ib_data', 'host'), config.getint('ib_data', 'port'),
                                config.getint('ib_data', 'client_id'))
contract: Contract = Contract()
contract.symbol = 'CL'
contract.secType = 'FUT'
contract.exchange = "NYMEX"
# contract.multiplier = 5000
contract.lastTradeDateOrContractMonth = "202104"
# contract: Contract = Contract()
# contract.symbol = '700'
# contract.secType = 'STK'
# contract.exchange = "SEHK"
# contract.currency = "HKD"
#
req = Request.new_request()
client.cli.reqTickByTickData(req.req_id, contract, "AllLast", 0, False)
# client.cli.reqContractDetails(req.req_id, contract)
