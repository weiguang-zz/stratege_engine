# import asyncio
# import logging
# import time
#
# from td.stream import TDStreamerClient
# from trading_calendars import get_calendar
#
# from se2.domain.account import *
# from se2.infras.td import *
# from se2.domain.time_series import *
#
# code = 'SPCE_STK_USD_SMART'
# account_name = "td_test3"
# repo: AccountRepo = BeanContainer.getBean(AccountRepo)
# acc: TDAccount = repo.find_one(account_name)
# if not acc:
#     acc = TDAccount(account_name, 200, "635212926")
#
# now = Timestamp.now(tz='Asia/Shanghai')
#
# # ts_repo: TimeSeriesRepo = BeanContainer.getBean(TimeSeriesRepo)
# # ts: TimeSeries = ts_repo.find_one("ibCurrentPrice")
# # ts.subscribe(None, [code])
# # time.sleep(3)
# # cp: CurrentPrice = ts.current_price([code])[code]
#
# # stream_session: TDStreamerClient = acc.client.create_streaming_session()
# # stream_session.account_activity()
#
#
# #
# # async def process():
# #     await stream_session.build_pipeline()
# #     while True:
# #         data = await stream_session.start_pipeline()
# #         logging.info('receive msg:{}'.format(data))
#         # print('=' * 20)
#         # print('Message Received:')
#         # print('-' * 20)
#         # print(data)
#         # print('-' * 20)
#         # print('')
#
#
# # def run():
# #     asyncio.run(process())
#
#
# # threading.Thread(target=lambda: asyncio.run(process()), name='process data').start()
#
# # time.sleep(20)
#
# # res = acc.client.get_user_principals(fields=['streamerConnectionInfo','streamerSubscriptionKeys','preferences','surrogateIds'])
# # for i in range(10):
# #     acc.stream_client.account_activity()
# #     time.sleep(1)
#
# # time.sleep(1200)
#
# order: Order = LimitOrder("SPCE_STK_USD_SMART", OrderDirection.BUY, 2, now, "test", 33, 33)
# # order.extended_time = True
# # acc.place_order(order)
# #
# # o = acc.client.get_orders("635212926", order.real_order_id)
#
#
# # res = acc.client.get_orders("635212926", "4548854101")
#
# # td_order = acc.change_to_td_order(order)
#
# # res = acc.client.modify_order("635212926", td_order, "4548854101")
#
# # res = acc.client.cancel_order("635212926", "4548854101")
#
# # stream_session.stream()
# print("done....")
import asyncio
import logging
import time

import websockets
from td.client import TDClient
from td.stream import TDStreamerClient

client = TDClient(
        client_id="E5O3Q884QBYRIBCMXNCJB7CSL5MPCGGD",
        redirect_uri="http://127.0.0.1:5000/callback",
        credentials_path="/Users/zhang/PycharmProjects/stratege_engine/new_interface/test_td/credential.json"
    )
client.login()
stream_client: TDStreamerClient = client.create_streaming_session()
async def test():
    # Create a connection.
    stream_client.connection = await websockets.client.connect(stream_client.websocket_url)
    await stream_client._send_message(stream_client._build_login_request())
    stream_client.account_activity()
    start = time.time()
    await stream_client._send_message(stream_client._build_data_request())

    await stream_client._send_message(stream_client._build_data_request())
    await stream_client._send_message(stream_client._build_data_request())
    await stream_client._send_message(stream_client._build_data_request())
    await stream_client._send_message(stream_client._build_data_request())
    await stream_client._send_message(stream_client._build_data_request())
    await stream_client._send_message(stream_client._build_data_request())
    print("cost:{}".format(time.time()-start))
    while True:
        msg = await stream_client.start_pipeline()
        print("{},receive message:{}".format(time.time(), msg))

asyncio.run(test())
