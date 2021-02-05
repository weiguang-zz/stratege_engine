import os
os.environ['config.dir'] = "/Users/zhangzheng/Quantitative/stratege_engine/interface3"

from se.infras.models import *
from cassandra.cqlengine.management import sync_table, sync_type
from se import config

ks_name = config.get("cassandra", "session_keyspace")

sync_type(ks_name, UserOrderExecutionModel)
sync_type(ks_name, UserOrderModel)
sync_type(ks_name, DataRecordModel)
sync_table(AccountModel)
sync_table(TimeSeriesModel)
sync_table(TimeSeriesDataModel)
