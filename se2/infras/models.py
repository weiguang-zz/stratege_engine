from cassandra.cqlengine import columns
from cassandra.cqlengine.columns import UserDefinedType
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.usertype import UserType


class UserOrderExecutionModel(UserType):
    __type_name__ = "user_order_execution3"
    id = columns.Text(required=True)
    version = columns.Integer(required=True)
    fee = columns.Float(required=True)
    quantity = columns.Float(required=True)
    price = columns.Float(required=True)
    time = columns.DateTime(required=True)
    real_order_id = columns.Text(required=True)


class CurrentPriceModel(UserType):
    __type_name__ = "current_price2"
    price = columns.Float()
    time = columns.DateTime()
    ask_price = columns.Float()
    bid_price = columns.Float()
    ask_size = columns.Integer()
    bid_size = columns.Integer()


class PriceChangeModel(UserType):
    __type_name__ = "price_change2"
    time = columns.DateTime()
    pre_price = columns.Float()
    after_price = columns.Float()
    current_price = UserDefinedType(CurrentPriceModel)


class BargainerModel(UserType):
    __type_name__ = 'bargainer'
    algo_type = columns.Text(required=True)
    current_price_history = columns.List(value_type=UserDefinedType(CurrentPriceModel), default=[])
    price_change_history = columns.List(value_type=UserDefinedType(PriceChangeModel), default=[])


class UserOrderModel(Model):
    __table_name__ = "user_order5"
    account_name = columns.Text(required=True, primary_key=True)
    place_time = columns.DateTime(required=True, primary_key=True, clustering_order='ASC')
    type = columns.Text(required=True)
    code = columns.Text(required=True)
    direction = columns.Text(required=True)
    quantity = columns.Float(required=True)
    status = columns.Text(required=True)
    reason = columns.Text(required=True)
    remark = columns.Text()
    real_order_id = columns.Text()
    failed_reason = columns.Text()
    cancel_reason = columns.Text()
    ideal_price = columns.Float()

    filled_start_time = columns.DateTime()
    filled_end_time = columns.DateTime()
    filled_quantity = columns.Float()
    filled_avg_price = columns.Float()
    fee = columns.Float()
    limit_price = columns.Float()
    stop_price = columns.Float()
    execution_map = columns.Map(key_type=columns.Text, value_type=UserDefinedType(UserOrderExecutionModel), default={})
    # price_change_history = columns.List(value_type=UserDefinedType(PriceChangeModel), default=[])
    bargainer = columns.UserDefinedType(BargainerModel)


class AccountModel(Model):
    __table_name__ = "account2"
    tp = columns.Text(required=True)
    name = columns.Text(required=True, primary_key=True)
    cash = columns.Float(required=True)
    initial_cash = columns.Float(required=True)
    positions = columns.Map(key_type=columns.Text, value_type=columns.Float, default={})
    account_id = columns.Text()
    history_net_value = columns.Map(key_type=columns.DateTime(), value_type=columns.Float(), default={})


class DataRecordModel(UserType):
    __type_name__ = "data_record2"
    code = columns.Text(required=True)
    start_time = columns.DateTime(required=True)
    end_time = columns.DateTime(required=True)


class TimeSeriesModel(Model):
    __table_name__ = "time_series2"
    name = columns.Text(primary_key=True, required=True)
    data_record = columns.Map(key_type=columns.Text(), value_type=columns.UserDefinedType(DataRecordModel), default={})


class TimeSeriesDataModel(Model):
    __table_name__ = "time_series_data2"
    type = columns.Text(required=True, primary_key=True)
    code = columns.Text(required=True, primary_key=True)
    visible_time = columns.DateTime(required=True, primary_key=True, clustering_order="ASC")
    data = columns.Text(required=True)
