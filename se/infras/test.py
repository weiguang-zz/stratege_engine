from cassandra.cqlengine import columns, connection
from cassandra.cqlengine.columns import UserDefinedType
from cassandra.cqlengine.management import sync_table, sync_type
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.usertype import UserType


# CREATE TABLE if not exists ts(
#     name varchar,
#     data_record varchar,
#     PRIMARY KEY(name));
#
# CREATE TABLE if not exists ts_data(
#   type varchar,
#   visible_time timestamp,
#   values varchar,
#   code  varchar,
#   PRIMARY KEY((type, code), visible_time));

# class DataRecord(UserType):
#     code = columns.Text(required=True)
#     start_time = columns.DateTime(required=True)
#     end_time = columns.DateTime(required=True)
#
#
# class TimeSeries(Model):
#     name = columns.Text(primary_key=True, required=True)
#     data_record = columns.Map(key_type=columns.Text(), value_type=columns.UserDefinedType(DataRecord), default={})
#
#
# class TimeSeriesData(Model):
#     type = columns.Text(required=True, primary_key=True)
#     code = columns.Text(required=True, primary_key=True)
#     visible_time = columns.DateTime(required=True, primary_key=True, clustering_order="ASC")
#     data = columns.Text(required=True)
#
#
# from se.infras.models import TimeSeriesModel

connection.setup(["127.0.0.1"], "data_center", protocol_version=3)
#
# # sync_type("data_center", DataRecord)
# sync_table(TimeSeries)
# sync_table(TimeSeriesData)

import json
import pandas as pd
a = {'a':1, 'b': 'sss'}
s = json.dumps(a)

json.loads(s)