# 该模块放置一些在所有应用中都需要解决的问题的解决方案（即领域模型），比如监控告警问题、稳定性问题（重试的实现）、依赖注入问题
from __future__ import annotations
import logging
import threading
import time
from configparser import ConfigParser
from email.header import Header
from email.mime.text import MIMEText
from enum import Enum
from smtplib import SMTP_SSL
from typing import *

from pandas._libs.tslibs.timedeltas import Timedelta
from pandas._libs.tslibs.timestamps import Timestamp

email_alarm_config: EmailAlarmConfig = None


class EmailAlarmConfig(object):
    def __init__(self, is_activate: bool, account_name: str, host_server: str, username: str, password: str,
                 sender_email: str, receiver: str):
        self.is_activate = is_activate
        self.account_name = account_name
        self.host_server = host_server
        self.username = username
        self.password = password
        self.sender_email = sender_email
        self.receiver = receiver


def initialize_email_alarm(config: EmailAlarmConfig):
    global email_alarm_config
    if email_alarm_config:
        raise RuntimeError("alarm配置已经初始化了")
    email_alarm_config = config


class BeanContainer(object):
    beans: Mapping[type, object] = {}

    @classmethod
    def getBean(cls, the_type: type):
        return cls.beans[the_type]

    @classmethod
    def register(cls, the_type: type, bean: object):
        cls.beans[the_type] = bean


def synchronized(func):
    func.__lock__ = threading.Lock()

    def synced_func(*args, **kws):
        with func.__lock__:
            return func(*args, **kws)

    return synced_func


def send_email(title: str, content: str):
    if not email_alarm_config or not email_alarm_config.is_activate:
        return

    def send_with_retry():
        retry_limit = 10
        for i in range(retry_limit):
            try:
                do_send_email(title, content)
                break
            except:
                import traceback
                logging.warning("发送邮件失败 {},邮件配置:{}".format(traceback.format_exc(), email_alarm_config.__dict__))
                import time
                time.sleep(10)

    threading.Thread(name='send_email', target=send_with_retry).start()


@synchronized
def do_send_email(title: str, content: str):
    # 登录
    logging.info("开始发送邮件")
    smtp = SMTP_SSL(email_alarm_config.host_server)
    smtp.set_debuglevel(0)
    smtp.ehlo(email_alarm_config.host_server)
    passwords = email_alarm_config.password.split(",")
    import random
    k = random.randint(0, len(passwords) - 1)
    password = passwords[k]
    smtp.login(email_alarm_config.username, password)
    title = '[{}]{}'.format(email_alarm_config.account_name, title)
    sender_email = email_alarm_config.sender_email
    receiver = email_alarm_config.receiver
    # 替换掉content中的< >字符
    content = content.replace('<', '[').replace('>', ']')
    msg = MIMEText(content, "plain", 'utf-8')
    msg["Subject"] = Header(title, 'utf-8')
    msg["From"] = sender_email
    msg["To"] = receiver
    smtp.sendmail(sender_email, receiver, msg.as_string())
    smtp.quit()


class AlarmLevel(Enum):
    NORMAL = 0
    ERROR = 1


class EscapeParam(object):
    def __init__(self, index: int, key: str, property_name: str = None):
        if not (index >= 0 and key):
            raise RuntimeError("wrong escape param")
        self.index = index
        self.key = key
        self.property_name = property_name


def do_alarm(target: str, level: AlarmLevel, params: str, return_obj: object, exception: str):
    title = "[{}]".format(target)
    if exception:
        title = '{}ERROR'.format(title)
    content = "params:{}, return_obj:{}, exception:{}". \
        format(params,
               return_obj.__dict__ if hasattr(return_obj, "__dict__") else return_obj,
               exception)
    send_email(title, content)


def build_params_str(*args, **kwargs):
    escape_params: List[EscapeParam] = None
    if 'escape_params' in kwargs:
        escape_params = kwargs.pop('escape_params')
    args_list = []
    kwargs_dict = {}
    for a in args:
        if hasattr(a, '__dict__'):
            args_list.append(a.__dict__.copy())
        else:
            args_list.append(a)
    for k in kwargs.keys():
        if hasattr(kwargs[k], '__dict__'):
            kwargs_dict[k] = kwargs[k].__dict__.copy()
        else:
            kwargs_dict[k] = kwargs[k]
    args_list_copy = args_list.copy()
    if escape_params:
        for escape_param in escape_params:
            if escape_param.key in kwargs_dict:
                if escape_param.property_name:
                    v = kwargs_dict[escape_param.key]
                    if isinstance(v, dict):
                        v.pop(escape_param.property_name)
                else:
                    kwargs_dict.pop(escape_param.key)
            else:
                if escape_param.index >= len(args_list) or escape_param.index < 0:
                    continue
                if escape_param.property_name:
                    v = args_list_copy[escape_param.index]
                    if isinstance(v, dict):
                        v.pop(escape_param.property_name)
                else:
                    args_list_copy.remove(args_list[escape_param.index])

    params = {'args': args_list_copy, 'kwargs': kwargs_dict}
    return str(params)


def alarm(level: AlarmLevel = AlarmLevel.NORMAL, target: str = None, freq: Timedelta = None,
          escape_params: List[EscapeParam] = None):
    last_alarm_time: Dict[str, Timestamp] = {}

    def wrapper(func: Callable):
        def inner_wrapper(*args, **kwargs):
            has_exception = False
            exception_str = None
            exception = None
            res = None
            try:
                res = func(*args, **kwargs)
            except Exception as e:
                import traceback
                exception_str = "{}".format(traceback.format_exc())
                has_exception = True
                exception = e

            if level == AlarmLevel.ERROR and not has_exception:
                pass
            else:
                new_kwargs = kwargs.copy()
                new_kwargs['escape_params'] = escape_params
                params_str = build_params_str(*args, **new_kwargs)
                if not freq:
                    do_alarm(target if target else func.__name__, level,
                             params_str,
                             res, exception_str)
                    last_alarm_time[target] = Timestamp.now(tz='Asia/Shanghai')
                else:
                    now = Timestamp.now(tz='Asia/Shanghai')
                    if (target not in last_alarm_time) or ((now - last_alarm_time[target]) > freq):
                        do_alarm(target if target else func.__name__, level,
                                 params_str,
                                 res, exception_str)
                        last_alarm_time[target] = now
                    else:
                        logging.info("由于频率控制，该告警将不会发出, target:{}, params:{}, res:{}, exception:{}".
                                     format(target if target else func.__name__,
                                            params_str,
                                            res, exception_str))

            if has_exception:
                raise exception
            else:
                return res

        return inner_wrapper

    return wrapper


def do_log(target_name: str = None, escape_params: List[EscapeParam] = None):
    def wrapper(func: Callable):
        def inner_wrapper(*args, **kwargs):
            new_kwargs = kwargs.copy()
            new_kwargs['escape_params'] = escape_params
            params_before = build_params_str(*args, **new_kwargs)
            is_exception = False
            params_after = None
            exception = None
            ret_obj = None
            start_time = time.time()
            try:
                ret_obj = func(*args, **kwargs)
                new_kwargs = kwargs.copy()
                new_kwargs['escape_params'] = escape_params
                params_after = build_params_str(*args, **new_kwargs)
            except Exception as e:
                exception = e
                is_exception = True

            log_dict = {'params_before': params_before, 'params_after': params_after,
                        "ret_obj": ret_obj, 'has_exception': is_exception, 'rt': time.time() - start_time}
            name = target_name if target_name else func.__name__
            if is_exception:
                logging.error("{}:{}".format(name, log_dict))
            else:
                logging.info("{}:{}".format(name, log_dict))

            if exception:
                raise exception
            else:
                return ret_obj

        return inner_wrapper

    return wrapper


class RetryError(Exception):
    pass


def retry(limit=3, interval: int = 0):
    if limit <= 1 or interval < 0:
        raise RuntimeError('wrong retry parameters')

    def wrapper(func: Callable):
        def inner_wrapper(*args, **kwargs):
            exception = None
            for k in range(limit):
                try:
                    if k > 0:
                        logging.info("方法:{}第{}次重试".format(func.__name__, k))
                    ret = func(*args, **kwargs)
                    return ret
                except RetryError as e:
                    exception = e
                    if interval > 0:
                        import time
                        time.sleep(interval)
                    continue
            raise exception

        return inner_wrapper

    return wrapper
