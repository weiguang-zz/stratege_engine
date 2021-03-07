import logging
from enum import Enum
from typing import *
import os

# os.environ['config.dir'] = "/Users/zhang/PycharmProjects/strategy_engine_v2/interface"
# # 如果没有日志目录的话，则创建
# if not os.path.exists("log"):
#     os.makedirs("log")
#
# from se import AccountRepo, BeanContainer
# from se.infras.ib import IBAccount

from pandas._libs.tslibs.timedeltas import Timedelta
from pandas._libs.tslibs.timestamps import Timestamp


class AlarmLevel(Enum):
    NORMAL = 0
    ERROR = 1


def do_alarm(target: str, level: AlarmLevel, params: Dict, return_obj: object, exception: str):
    title = "[{}]".format(target)
    if level == AlarmLevel.ERROR:
        title = '{}ERROR'.format(title)
    content = "params:{}, return_obj:{}, exception:{}". \
        format(params,
               return_obj.__dict__ if hasattr(return_obj, "__dict__") else return_obj,
               exception)
    from se.domain2.domain import send_email
    send_email(title, content)


def alarm(level: AlarmLevel = AlarmLevel.NORMAL, target: str = None, freq: Timedelta = None):
    last_alarm_time: Mapping[str, Timestamp] = {}

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
                params_str = build_params_str(*args, **kwargs)
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


class EscapeParam(object):
    def __init__(self, index: int, key: str, property_name: str = None):
        if not (index > 0 and key):
            raise RuntimeError("wrong escape param")
        self.index = index
        self.key = key
        self.property_name = property_name


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
            try:
                ret_obj = func(*args, **kwargs)
                new_kwargs = kwargs.copy()
                new_kwargs['escape_params'] = escape_params
                params_after = build_params_str(*args, **new_kwargs)
            except RuntimeError as e:
                exception = e
                is_exception = True

            log_dict = {'params_before': params_before, 'params_after': params_after,
                        "ret_obj": ret_obj, 'has_exception': is_exception}
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
                    v = args_list[escape_param.index]
                    if isinstance(v, dict):
                        v.pop(escape_param.property_name)
                else:
                    args_list.pop(escape_param.index)

    params = {'args': args_list, 'kwargs': kwargs_dict}
    return str(params)


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
                except RuntimeError as e:
                    exception = e
                    if interval > 0:
                        import time
                        time.sleep(interval)
                    continue
            raise exception
        return inner_wrapper

    return wrapper


@do_log(target_name="测试", escape_params=[EscapeParam(index=2, key='o', property_name='yyy')])
@retry(limit=5, interval=2)
def test(p1, p2, o):
    o.add = 'add'
    # return 'sss'
    raise RuntimeError('error')


class A(object):
    pass


logging.basicConfig(level=logging.INFO)
o = A()
o.xxx = 'xxx'
o.yyy = 'yyyy'

test('param1', 'param2', o=o)
