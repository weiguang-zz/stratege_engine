from cassandra.cqlengine.management import sync_table, sync_type

from se2 import config
from se2.infras.models import *

ks_name = config.get("cassandra", "session_keyspace")

sync_type(ks_name, UserOrderExecutionModel)
sync_type(ks_name, CurrentPriceModel)
sync_type(ks_name, PriceChangeModel)
sync_type(ks_name, DataRecordModel)
sync_table(AccountModel)
sync_table(TimeSeriesModel)
sync_table(TimeSeriesDataModel)
sync_table(UserOrderModel)
