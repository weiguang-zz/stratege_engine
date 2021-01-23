# 初始化配置
import os
from configparser import ConfigParser

config_file_name = 'config.ini'
if not os.path.exists(config_file_name):
    raise RuntimeError("需要配置文件config.ini")
config = ConfigParser()
config.read(config_file_name)