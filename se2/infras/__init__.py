import logging.config
import os

import yaml
from cassandra.cqlengine import connection

from se2.domain import common
from se2.infras.ib2 import *
from se2.infras.repos import *
from se2.infras.td import *

# 注册repo的实现类
BeanContainer.register(TimeSeriesRepo, TimeSeriesRepoImpl())
BeanContainer.register(AccountRepo, AccountRepoImpl())
BeanContainer.register(OrderRepo, OrderRepoImpl())
BeanContainer.register(TSDataRepo, TSDataRepoImpl())

# 读取配置文件， 默认从运行目录读取config.ini, 用户可以通过config.dir环境变量来覆盖
if not os.getenv("config.dir"):
    config_file = 'config.ini'
else:
    config_file = "{}/config.ini".format(os.getenv("config.dir"))
config = ConfigParser()
config.read(config_file)
logging.info("初始化应用配置成功")

# 日志初始化
try:
    log_config_file = config.get("log", 'config_file')
except:
    log_config_file = 'log.yaml'
if os.path.exists(log_config_file):
    logging.config.dictConfig(yaml.load(open(log_config_file), Loader=yaml.SafeLoader))
    logging.info("初始化日志配置成功")
else:
    logging.basicConfig(level=logging.INFO)
    logging.info("没有log的配置文件,将使用默认配置")

# 初始化告警配置
if 'alarm' in config.sections():
    common.initialize_email_alarm(
        EmailAlarmConfig(config.getboolean('alarm', 'is_activate'), config.get('alarm', 'account_name'),
                         config.get('alarm', 'host_server'), config.get('alarm', 'username'),
                         config.get('alarm', 'password'), config.get('alarm', 'sender_email'),
                         config.get('alarm', 'receiver')))

# 初始化DB连接
connection.setup(config.get("cassandra", "contact_points").split(","),
                 config.get("cassandra", "session_keyspace"), protocol_version=3,
                 port=config.getint("cassandra", "port"))

# ib初始化
if 'ib' in config.sections():
    ib2.initialize(config.get("ib", "host"), config.getint("ib", 'port'),
                  config.getint('ib', 'client_id'))

# td初始化
if 'td' in config.sections():
    td.initialize(config.get('td', 'client_id'), config.get('td', 'redirect_uri'), config.get('td', 'credentials_path'))


logging.info("应用初始化成功")
