import logging
import os
from email import encoders
from email.header import Header
from email.mime.text import MIMEText
from smtplib import SMTP_SSL

from ibapi.contract import Contract
from ibapi.tag_value import TagValue
from pandas._libs.tslibs.timestamps import Timestamp



os.environ['config.dir'] = "/Users/zhang/PycharmProjects/strategy_engine_v2/interface"
# 如果没有日志目录的话，则创建
if not os.path.exists("log"):
    os.makedirs("log")

from se.domain2.time_series.time_series import TimeSeries, TimeSeriesRepo, TimeSeriesSubscriber, TSData
from se.domain2.engine.engine import DataPortal
from se import config, BeanContainer
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
# req = Request.new_request()
# # client.cli.reqTickByTickData(req.req_id, contract, "AllLast", 0, False)
# # client.cli.reqContractDetails(req.req_id, contract)
# client.cli.reqMktData(req.req_id, contract, '', False, False, None)
code = 'CL_FUT_USD_NYMEX_202104'
# code = 'GSX_STK_USD_SMART'
dp: DataPortal = DataPortal(is_backtest=False, ts_type_name_for_current_price='ibMarketData',
                            subscribe_codes=[code])
# cp = dp.current_price([code], Timestamp.now(tz='Asia/Shanghai'))
# print("cp:{}".format(cp))

class MySub(TimeSeriesSubscriber):

    def on_data(self, data: TSData):
        print(str(data.__dict__))


ts_repo: TimeSeriesRepo = BeanContainer.getBean(TimeSeriesRepo)
ts: TimeSeries = ts_repo.find_one('ibMarketData')
ts.subscribe(MySub(), ['CL_FUT_USD_NYMEX_202104'])

while True:
    import time
    time.sleep(2)
    cp = ts.func.current_price([code])
    if code in cp:
        print("当前价格:{}".format(cp[code].__dict__))
