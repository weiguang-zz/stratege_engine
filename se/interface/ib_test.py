import logging
import os
import threading
import time

from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.wrapper import EWrapper
from pandas._libs.tslibs.timestamps import Timestamp

from se.domain2.domain import BeanContainer
from se.domain2.time_series.time_series import TimeSeriesRepo, TimeSeries, HistoryDataQueryCommand


def SetupLogger():
    if not os.path.exists("log"):
        os.makedirs("log")

    time.strftime("application.%Y%m%d_%H%M%S.log")

    recfmt = '(%(threadName)s) %(asctime)s.%(msecs)03d %(levelname)s %(filename)s:%(lineno)d %(message)s'
    timefmt = '%y%m%d_%H:%M:%S'

    # logging.basicConfig( level=logging.DEBUG,
    #                    format=recfmt, datefmt=timefmt)
    logging.basicConfig(filename=time.strftime("log/application.%y%m%d.log"),
                        filemode="a",
                        level=logging.INFO,
                        format=recfmt, datefmt=timefmt)
    logger = logging.getLogger()
    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter(fmt=recfmt, datefmt=timefmt))
    console.setLevel(logging.DEBUG)
    logger.addHandler(console)

# SetupLogger()

# wrapper = EWrapper()
# cli = EClient(wrapper)
# cli.connect("192.168.0.221", 4002, 25)
# if cli.connState == EClient.CONNECTED:
#     threading.Thread(name="ib_msg_consumer", target=cli.run).start()
#
# cont = Contract()
# cont.symbol = 'SPCE'
# cont.currency = "USD"
# cont.exchange = "SMART"
# cont.secType = "STK"
# cli.reqContractDetails(1000, cont)
# cli.reqAccountUpdates(True, "DU268989")

import se.infras

ts_repo:TimeSeriesRepo = BeanContainer.getBean(TimeSeriesRepo)

ts:TimeSeries = ts_repo.find_one("ibTick")
command = HistoryDataQueryCommand(start=Timestamp("2020-01-20", tz='Asia/Shanghai'),
                                  end=Timestamp.now(tz='Asia/Shanghai'), codes=['SPCE_STK_USD_SMART'],
                                  window=10)
datas = ts.history_data(command)

print("done")


print("done")

