import time

from se2.domain.time_series import *
from se2.infras.td import *

code = 'SPCE_STK_USD_SMART'
account_name = "td_test4"
repo: AccountRepo = BeanContainer.getBean(AccountRepo)
acc: TDAccount = repo.find_one(account_name)
if not acc:
    acc = TDAccount(account_name, 200, "635212926")

ts_repo: TimeSeriesRepo = BeanContainer.getBean(TimeSeriesRepo)
realtime_ts: TimeSeries = ts_repo.find_one("ibCurrentPrice")
realtime_ts.subscribe(None, [code])
time.sleep(2)

bargin_algo:BarginAlgo = DefaultBarginAlgo(acc, realtime_ts, 1, -0.01)
order: Order = LimitOrder(code, OrderDirection.BUY, 1, Timestamp.now(), "test", 33, None,
                          bargin_algo=bargin_algo)
order.extended_time = True

acc.place_order(order)
# res = acc.client.cancel_order(acc.account_id, "4481916136")

# order_repo: OrderRepo = BeanContainer.getBean(OrderRepo)
# orders = order_repo.find_by_account_name("test")

print("done")