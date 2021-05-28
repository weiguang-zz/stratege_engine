import asyncio
import logging
import time

from td.stream import TDStreamerClient
from trading_calendars import get_calendar

from se2.domain.account import *
from se2.infras.td import *
from se2.domain.time_series import *

code = 'SPCE_STK_USD_SMART'
account_name = "td_test3"
repo: AccountRepo = BeanContainer.getBean(AccountRepo)
acc: TDAccount = repo.find_one(account_name)
if not acc:
    acc = TDAccount(account_name, 200, "635212926")

now = Timestamp.now(tz='Asia/Shanghai')

# ts_repo: TimeSeriesRepo = BeanContainer.getBean(TimeSeriesRepo)
# ts: TimeSeries = ts_repo.find_one("ibCurrentPrice")
# ts.subscribe(None, [code])
# time.sleep(3)
# cp: CurrentPrice = ts.current_price([code])[code]

# stream_session: TDStreamerClient = acc.client.create_streaming_session()
# stream_session.account_activity()


#
# async def process():
#     await stream_session.build_pipeline()
#     while True:
#         data = await stream_session.start_pipeline()
#         logging.info('receive msg:{}'.format(data))
        # print('=' * 20)
        # print('Message Received:')
        # print('-' * 20)
        # print(data)
        # print('-' * 20)
        # print('')


# def run():
#     asyncio.run(process())


# threading.Thread(target=lambda: asyncio.run(process()), name='process data').start()

# time.sleep(5)
order: Order = LimitOrder("SPCE_STK_USD_SMART", OrderDirection.BUY, 2, now, "test", 33, 33)
order.extended_time = True
acc.place_order(order)

# stream_session.stream()
print("done....")
