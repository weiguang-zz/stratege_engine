import logging.config
import os
from configparser import ConfigParser

import yaml
from cassandra.cqlengine import connection

from se.domain2.account.account import AccountRepo
from se.domain2.domain import BeanContainer
from se.domain2.time_series.time_series import TSFunctionRegistry, TimeSeriesRepo
from se.infras.ib import IBMinBar, IBTick, IBAdjustedDailyBar, IBMarketData
from se.infras.repos import TimeSeriesRepoImpl, AccountRepoImpl

BeanContainer.register(TimeSeriesRepo, TimeSeriesRepoImpl())
BeanContainer.register(AccountRepo, AccountRepoImpl())

if not os.getenv("config.dir"):
    raise RuntimeError("没有配置config.dir")

# 初始化日志配置
log_config = "{}/log.yaml".format(os.getenv("config.dir"))
if os.getenv("config.log"):
    log_config = os.getenv("config.log")

if os.path.exists(log_config):
    logging.config.dictConfig(yaml.load(open(log_config), Loader=yaml.SafeLoader))
    logging.info("初始化日志配置成功")
else:
    logging.basicConfig(level=logging.INFO)
    logging.info("没有log的配置文件,将使用默认配置")

# 初始化应用配置
config_file = "{}/config_default.ini".format(os.getenv("config.dir"))
if not os.path.exists(config_file):
    raise RuntimeError("需要配置文件config_default.ini")
config = ConfigParser()
config.read(config_file)
# 如果运行目录存在config.ini的话，则替换默认的配置
if os.path.exists("config.ini"):
    config.read("config.ini")
logging.info("初始化应用配置成功")

# 初始化DB连接
connection.setup(config.get("cassandra", "contact_points").split(","),
                 config.get("cassandra", "session_keyspace"), protocol_version=3,
                 port=config.getint("cassandra", "port"))

# 注册时序类型
TSFunctionRegistry.register(IBMinBar(config.get("ib_data", "host"), config.getint("ib_data", 'port'),
                                     config.getint('ib_data', 'client_id')))
TSFunctionRegistry.register(IBTick(config.get("ib_data", "host"), config.getint("ib_data", 'port'),
                                   config.getint('ib_data', 'client_id')))
TSFunctionRegistry.register(IBMarketData(config.get("ib_data", "host"), config.getint("ib_data", 'port'),
                                         config.getint('ib_data', 'client_id')))
TSFunctionRegistry.register(IBAdjustedDailyBar(config.get("ib_data", "host"), config.getint("ib_data", 'port'),
                                               config.getint('ib_data', 'client_id')))
