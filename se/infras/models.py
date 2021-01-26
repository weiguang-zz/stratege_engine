from cassandra.cqlengine import columns
from cassandra.cqlengine.columns import UserDefinedType
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.usertype import UserType


class UserOrderExecutionModel(UserType):
    __type_name__ = "user_order_execution"
    id = columns.Text(required=True)
    version = columns.Integer(required=True)
    commission = columns.Text(required=True)
    direction = columns.Text(required=True)
    filled_quantity = columns.Float()
    filled_avg_price = columns.Float()
    filled_start_time = columns.DateTime()
    filled_end_time = columns.DateTime()
    attributes = columns.Map(key_type=columns.Text, value_type=columns.Text)


class UserOrderModel(UserType):
    __type_name__ = "user_order"
    type = columns.Text(required=True)
    code = columns.Text(required=True)
    direction = columns.Text(required=True)
    quantity = columns.Float(required=True)
    place_time = columns.DateTime(required=True)
    status = columns.Text(required=True)
    filled_start_time = columns.DateTime()
    filled_end_time = columns.DateTime()
    filled_quantity = columns.Float()
    filled_avg_price = columns.Float()
    fee = columns.Float()
    delay_time = columns.Text()
    limit_price = columns.Float()
    cross_price = columns.Float()
    cross_direction = columns.Text()
    execution_map = columns.Map(key_type=columns.Text, value_type=UserDefinedType(UserOrderExecutionModel), default={})


class AccountModel(Model):
    __table_name__ = "account"
    tp = columns.Text(required=True)
    name = columns.Text(required=True, primary_key=True)
    cash = columns.Float(required=True)
    initial_cash = columns.Float(required=True)
    positions = columns.Map(key_type=columns.Text, value_type=columns.Float, default={})
    history_net_value = columns.Map(key_type=columns.DateTime(), value_type=columns.Float(), default={})
    orders = columns.List(value_type=UserDefinedType(UserOrderModel))


class DataRecordModel(UserType):
    __type_name__ = "data_record"
    code = columns.Text(required=True)
    start_time = columns.DateTime(required=True)
    end_time = columns.DateTime(required=True)


class TimeSeriesModel(Model):
    __table_name__ = "time_series"
    name = columns.Text(primary_key=True, required=True)
    data_record = columns.Map(key_type=columns.Text(), value_type=columns.UserDefinedType(DataRecordModel), default={})


class TimeSeriesDataModel(Model):
    __table_name__ = "time_series_data"
    type = columns.Text(required=True, primary_key=True)
    code = columns.Text(required=True, primary_key=True)
    visible_time = columns.DateTime(required=True, primary_key=True, clustering_order="ASC")
    data = columns.Text(required=True)
