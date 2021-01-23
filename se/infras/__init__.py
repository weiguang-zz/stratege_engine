import os
from configparser import ConfigParser

from cassandra.cqlengine import connection

from se.domain2.account.account import AccountRepo
from se.domain2.domain import BeanContainer
from se.infras.ib import IBMinBar, IBTick
from se.domain2.time_series.time_series import TSFunctionRegistry, TimeSeriesRepo
from se.infras.repos import TimeSeriesRepoImpl, AccountRepoImpl

BeanContainer.register(TimeSeriesRepo, TimeSeriesRepoImpl())
BeanContainer.register(AccountRepo, AccountRepoImpl())

from se.domain2 import config

# 初始化DB连接
connection.setup(config.get("cassandra", "contact_points").split(","),
                 config.get("cassandra", "session_keyspace"), protocol_version=3,
                 port=config.getint("cassandra", "port"))

# 注册时序类型
TSFunctionRegistry.register(IBMinBar(config.get("ib", "host"), config.getint("ib", 'port'),
                                     config.getint('ib', 'client_id')))
TSFunctionRegistry.register(IBTick(config.get("ib", "host"), config.getint("ib", 'port'),
                                   config.getint('ib', 'client_id')))
