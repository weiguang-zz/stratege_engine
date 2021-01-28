from abc import *
from enum import Enum
from typing import *

from pandas import Timestamp, DatetimeIndex, DataFrame
from pandas import Timedelta
from trading_calendars import TradingCalendar
from threading import Thread
import logging
import time

from se.domain.data_portal import TSDataReader, StreamDataCallback, TSData


class EventType(Enum):
    TIME = 0
    ACCOUNT = 1
    DATA = 2


class AccountEventType(Enum):
    FILLED = 0
    CANCELED = 1


class Event(object):

    def __init__(self, event_type: EventType, sub_type, visible_time: Timestamp, data: object):
        if not visible_time.tz:
            raise RuntimeError("事件的可见时间必须有时区")
        # 统一使用中国标准时间
        visible_time = visible_time.tz_convert("Asia/Shanghai")
        self.visible_time = visible_time
        self.event_type = event_type
        self.sub_type = sub_type
        self.data = data

    def __lt__(self, other):
        if self.visible_time == other.visible_time:
            # 账户事件放在最前面，因为账户事件是延后的，撮合时间事件放在最后面，因为撮合使用的是未来的数据
            if self.event_type == EventType.ACCOUNT:
                return True
            if self.event_type == EventType.TIME and self.sub_type == 'match_time':
                return False
            if other.event_type == EventType.TIME and other.sub_type == 'match_time':
                return True
            return False
        else:
            return self.visible_time < other.visible_time

    def __str__(self):
        return '[Event]: event_type:{event_type}, sub_type:{sub_type}, visible_time:{visible_time}, data:{data}'. \
            format(event_type=self.event_type, sub_type=self.sub_type, visible_time=self.visible_time, data=self.data)


class EventBus(object):

    def __init__(self):
        self.subscribers = []

    def publish(self, event: Event):
        for subscribe in self.subscribers:
            subscribe.on_event(event)

    def register(self, subscribe):
        if not subscribe.on_event:
            raise RuntimeError("监听者必须实现onEvent方法")
        self.subscribers.append(subscribe)


class EventSubscriber(object, metaclass=ABCMeta):
    @abstractmethod
    def on_event(self, event: Event):
        pass


class EventProducer(metaclass=ABCMeta):

    @abstractmethod
    def start_listen(self, subscriber: EventSubscriber):
        pass

    @abstractmethod
    def get_events_on_history(self, visible_time_start: Timestamp, visible_time_end: Timestamp):
        pass


class Rule(metaclass=ABCMeta):
    @abstractmethod
    def is_match(self, dt: Timestamp):
        pass


class EveryDay(Rule):
    def is_match(self, dt: Timestamp):
        return True


class MarketOpen(Rule):
    def __init__(self, calendar: TradingCalendar, offset=0):
        self.offset = offset
        self.calendar = calendar
        self.event_times = DatetimeIndex(self.calendar.opens.values, tz='UTC') + Timedelta(minutes=offset - 1)

    def is_match(self, dt: Timestamp):
        if dt in self.event_times:
            return True
        return False


class MarketClose(Rule):
    def __init__(self, calendar: TradingCalendar, offset):
        self.offset = offset
        self.calendar = calendar
        self.event_times = DatetimeIndex(self.calendar.closes.values, tz='UTC') + Timedelta(minutes=offset)

    def is_match(self, dt: Timestamp):
        if dt in self.event_times:
            return True
        return False


class DateRules(object):
    @classmethod
    def every_day(cls):
        return EveryDay()


class TimeRules(object):
    @classmethod
    def market_open(cls, calendar: TradingCalendar, offset=0):
        return MarketOpen(calendar, offset)

    @classmethod
    def market_close(cls, calendar: TradingCalendar, offset=0):
        return MarketClose(calendar, offset)


class TimeEventCondition(object):
    def __init__(self, date_rule: Rule, time_rule: Rule, name: str):
        self.name = name
        self.date_rule = date_rule
        self.time_rule = time_rule

    def is_match(self, time: Timestamp):
        return self.date_rule.is_match(time) and self.time_rule.is_match(time)


class TimeEventThread(Thread):
    def __init__(self, subscriber: EventSubscriber, time_event_conditions: List[TimeEventCondition]):
        super().__init__()
        self.name = "time_event_thread"
        self.subscriber = subscriber
        self.time_event_conditions = time_event_conditions

    def run(self) -> None:
        try:
            while True:
                t: Timestamp = Timestamp.now(tz='Asia/Shanghai')
                t = t.round(freq=Timedelta(seconds=1))
                logging.info("当前时间:{}".format(t))
                for cond in self.time_event_conditions:
                    if cond.is_match(t):
                        event = Event(EventType.TIME, cond.name, t, {})
                        self.subscriber.on_event(event)
                time.sleep(0.8)
        except RuntimeError as e:
            import traceback
            logging.error("{}".format(traceback.format_exc()))


class TimeEventProducer(EventProducer):

    def get_events_on_history(self, visible_time_start: Timestamp, visible_time_end: Timestamp):
        events = []
        delta = Timedelta(minutes=1)
        p = visible_time_start
        while p <= visible_time_end:
            for cond in self.time_event_conditions:
                if cond.is_match(p):
                    events.append(Event(EventType.TIME, cond.name, p, {}))
            p += delta
        return events

    def __init__(self, time_event_conditions: List[TimeEventCondition]):
        self.time_event_conditions = time_event_conditions

    def start_listen(self, subscriber: EventSubscriber):
        # 启动线程来产生时间事件
        TimeEventThread(subscriber, self.time_event_conditions).start()


class TSDataEventProducer(EventProducer, StreamDataCallback):
    """
    默认的数据事件产生器，要定义一个数据事件产生器，只需要指定供应商名和时序类型名即可
    """

    def on_data(self, data: TSData):
        pass

    def start_listen(self, subscriber):
        self.data_reader.listen(self)

    def get_events_on_history(self, visible_time_start: Timestamp, visible_time_end: Timestamp):
        df: DataFrame = self.data_reader.history_data(self.codes, visible_time_start, visible_time_end)
        events = []
        for _, row in df.iterrows():
            data: Dict = row.to_dict()
            visible_time = row.name[0]
            data['code'] = row.name[1]
            event = Event(event_type=EventType.DATA, sub_type=self.sub_type,
                          visible_time=visible_time, data=data)
            events.append(event)
        return events

    def __init__(self, provider_name: str, ts_type_name: str, codes: List[str]):
        data_reader: TSDataReader = TSDataReader(provider_name, ts_type_name)
        self.sub_type = "{provider_name}:{ts_type_name}".format(provider_name=provider_name, ts_type_name=ts_type_name)
        self.data_reader = data_reader
        self.codes = codes


if __name__ == "__main__":
    from trading_calendars import get_calendar

    calendar: TradingCalendar = get_calendar("NYSE")
    print("done")
