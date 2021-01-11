import logging
import time
from abc import ABCMeta, abstractmethod
from threading import Thread
from typing import *

from pandas import DatetimeIndex
from pandas._libs.tslibs.timedeltas import Timedelta
from pandas._libs.tslibs.timestamps import Timestamp
from trading_calendars import TradingCalendar

from se.domain2.account.account import AbstractAccount, BacktestAccount, Bar, Tick, OrderCallback
from se.domain2.time_series.time_series import TimeSeriesRepo, HistoryDataQueryCommand, TimeSeriesSubscriber, TSData
from se.infras.models import AccountModel
from __future__ import annotations

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
    def __init__(self, calendar: TradingCalendar, offset=0):
        self.offset = offset
        self.calendar = calendar
        self.event_times = DatetimeIndex(self.calendar.closes.values, tz='UTC') + Timedelta(minutes=offset)

    def is_match(self, dt: Timestamp):
        if dt in self.event_times:
            return True
        return False


class Scope(object):
    def __init__(self, codes: List[str], trading_calendar: TradingCalendar):
        self.codes = codes
        self.trading_calendar = trading_calendar


class Event(object):
    def __init__(self, name: str, visible_time: Timestamp, data: object):
        self.name = name
        self.visible_time = visible_time
        self.data = data


class EventDefinition(metaclass=ABCMeta):
    def __init__(self, name, callback: Callable[[Event, AbstractAccount, object], List[Event]]):
        self.name = name


class TimeEventDefinition(EventDefinition):
    def __init__(self, name: str, date_rule: Rule, time_rule: Rule):
        super().__init__(name)
        self.date_rule = date_rule
        self.time_rule = time_rule

    def is_match(self, dt: Timestamp):
        return self.date_rule.is_match(dt) and self.time_rule.is_match(dt)


class DataEventDefinition(EventDefinition):
    def __init__(self, name: str, ts_type_name: str,
                 is_bar: bool = False, bar_open_as_tick: bool = False):
        super().__init__(name)
        self.ts_type_name = ts_type_name
        self.is_bar = is_bar
        self.bar_open_as_tick = bar_open_as_tick


class EventSubscriber(metaclass=ABCMeta):
    @abstractmethod
    def on_event(self, event: Event):
        pass


class EventProducer(metaclass=ABCMeta):
    @abstractmethod
    def history_events(self, scope: Scope, start: Timestamp, end: Timestamp) -> List[Event]:
        pass

    def subscribe(self, subscriber: EventSubscriber):
        self.subscriber = subscriber

    @abstractmethod
    def start(self, scope: Scope):
        pass

    def __init__(self, event_definitions: List[EventDefinition]):
        self.event_definitions = event_definitions
        self.subscriber = None


class TimeEventThread(Thread):
    def __init__(self, subscriber: EventSubscriber, time_event_conditions: List[TimeEventDefinition]):
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
                    if cond.date_rule.is_match(t) and cond.time_rule.is_match(t):
                        event = Event(cond.name, t, {})
                        self.subscriber.on_event(event)
                time.sleep(0.8)
        except RuntimeError as e:
            logging.error('error', e)


class TimeEventProducer(EventProducer):
    def start(self, scope: Scope):
        # 启动线程来产生时间事件
        TimeEventThread(self.subscriber, self.time_event_conditions).start()

    def history_events(self, scope: Scope, start: Timestamp, end: Timestamp):
        events = []
        delta = Timedelta(minutes=1)
        p = start
        while p <= end:
            for cond in self.time_event_conditions:
                if cond.is_match(p):
                    events.append(cond.name, p, {})
            p += delta
        return events

    def __init__(self, event_definitions: List[TimeEventDefinition]):
        for ed in event_definitions:
            if not isinstance(ed, TimeEventDefinition):
                raise RuntimeError("非法的事件定义")
        super().__init__(event_definitions)


class DataEventProducer(EventProducer, TimeSeriesSubscriber):
    def on_data(self, data: TSData):
        ed = self.ts_type_name_to_ed[data.ts_type_name]
        self.subscriber.on_event(Event(name=ed.name, visible_time=data.visible_time, data=data))

    def start(self, scope: Scope):
        for ed in self.event_definitions:
            ts = TimeSeriesRepo.find_one(ed.name)
            ts.subscribe(self, scope.codes)

    def history_events(self, scope: Scope, start: Timestamp, end: Timestamp):
        total_events = []
        for ed in self.event_definitions:
            if not isinstance(ed, DataEventDefinition):
                raise RuntimeError("wrong event definition")
            ts = TimeSeriesRepo.find_one(ed.ts_type_name)
            command = HistoryDataQueryCommand(start, end, scope.codes)
            df = ts.history_data(command)
            for (visible_time, code), values in df.iterrows():
                data = values
                if ed.is_bar:
                    data = Bar(code=code, start_time=values['date'], visible_time=visible_time,
                               open_price=values['open'], high_price=values['high'], low_price=values['low'],
                               close_price=values['close'])
                total_events.append(Event(ed.name, visible_time, data))
                if ed.bar_open_as_tick:
                    total_events.append((Event(ed.name, values['date'], Tick(code=code, visible_time=values['date'],
                                                                             price=values['open'], size=-1))))
        return total_events

    def __init__(self, event_definitions: List[DataEventDefinition]):
        for ed in event_definitions:
            if not isinstance(ed, DataEventDefinition):
                raise RuntimeError("非法的事件定义")
        super().__init__(event_definitions)
        self.ts_type_name_to_ed = {ed.ts_type_name: ed for ed in event_definitions}


class DataPortal(object):

    def history_data(self, ts_type_name, codes, end, window):
        command = HistoryDataQueryCommand(None, end, codes, window)
        ts = TimeSeriesRepo.find_one(ts_type_name)
        return ts.history_data(command)

    def __init__(self, is_backtest: bool, ts_type_name_for_current_price: str = None):
        if not is_backtest:
            if not ts_type_name_for_current_price:
                raise RuntimeError("need ts_type_name_for_current_price")

        self.ts_type_name_for_current_price = ts_type_name_for_current_price
        self.is_backtest = is_backtest
        self._current_price_map = {}

    def current_price(self, codes: List[str]):
        """
        在实盘或者回测的时候，获取当前价格的方式不同，实盘的时候，依赖某个时序类型来获取最新的价格。 但是在回测的时候，会从缓存中获取，
        缓存是撮合的时候构建的
        :param codes:
        :return:
        """
        if self.is_backtest:
            return {code: self._current_price_map[code] for code in codes}
        else:
            ts = TimeSeriesRepo.find_one(self.ts_type_name_for_current_price)
            return ts.current_price(codes)

    def set_current_price(self, code, cp):
        self._current_price_map[code] = cp




class AbstractStrategy(OrderCallback, metaclass=ABCMeta):

    @abstractmethod
    def initialize(self, engine: Engine):
        pass

    def __init__(self, scope: Scope):
        self.scope = scope


class EventLine(object):

    def __init__(self):
        self.events = []

    def add_all(self, events: List[Event]):
        self.events.extend(events)
        self.events.sort()

    def pop_event(self) -> Event:
        if len(self.events) > 0:
            return self.events.pop(0)
        else:
            return None


class Engine(EventSubscriber):

    def register_event(self, event_definition: EventDefinition,
                       callback: Callable[[Event, AbstractAccount, DataPortal], None]):
        if event_definition.name in self.callback_map:
            raise RuntimeError("wrong name")
        self.callback_map[event_definition.name] = callback
        self.event_definitions.append(event_definition)

    def on_event(self, event: Event):
        """
        接收实时事件
        :param event:
        :return:
        """
        callback = self.context.callback_for(event.name)
        callback(event, self.account, self.data_portal)

    def calc_net_value(self, event: Event, account: AbstractAccount, data_portal: DataPortal, context: Context):
        if len(account.positions) > 0:
            current_price: Mapping[str, float] = data_portal.current_price(list(account.positions.keys()))
            account.calc_net_value(current_price)

    def match(self, event: Event, account: AbstractAccount, data_portal: DataPortal, context: Context):
        if event.name != "match":
            raise RuntimeError("wrong event name")
        if isinstance(event.data, Bar):
            data_portal.set_current_price(event.data.code, event.data.close_price)
        if isinstance(event.data, Tick):
            data_portal.set_current_price(event.data.code, event.data.price)
        else:
            raise RuntimeError("wrong event data")

        account.match(event.data, context)

    def run_backtest(self, strategy: AbstractStrategy, scope: Scope, start: Timestamp, end: Timestamp,
                     initial_cash: float,
                     account_name: str):
        # 检查account_name是否唯一
        if not self.is_unique_account(account_name):
            raise RuntimeError("account name重复")
        context = Context(scope)
        data_portal = DataPortal(True)
        strategy.initialize(context)
        context.register_event(TimeEventDefinition("calc_net_value",
                                                   date_rule=EveryDay(),
                                                   time_rule=MarketClose(scope.trading_calendar, 30)),
                               self.calc_net_value)
        context.register_event(DataEventDefinition("match", "ibMinBar", True, True),
                               self.match)

        time_event_definitions: List[TimeEventDefinition] = context.get_time_event_definitions()
        data_event_definitions: List[DataEventDefinition] = context.get_data_event_definitions()

        event_line = EventLine()
        if len(time_event_definitions) > 0:
            tep = TimeEventProducer(time_event_definitions)
            event_line.add_all(tep.history_events(start, end))
        if len(data_event_definitions) > 0:
            dep = DataEventProducer(data_event_definitions)
            event_line.add_all(dep.history_events(start, end))

        account = BacktestAccount(account_name, initial_cash, strategy)
        event: Event = event_line.pop_event()
        while event is not None:
            callback = context.callback_for(event.name)
            callback(event, account, data_portal, context)
            event = event_line.pop_event()
        # 存储以便后续分析用
        account.save()
        return account

    def run(self, strategy: AbstractStrategy, scope: Scope, account: AbstractAccount):
        context = Context(scope)
        strategy.initialize(context)
        context.register_event(TimeEventDefinition("calc_net_value",
                                                   date_rule=EveryDay(),
                                                   time_rule=MarketClose(strategy.trading_calendar, 30)),
                               self.calc_net_value)
        time_event_definitions: List[TimeEventDefinition] = context.get_time_event_definitions()
        data_event_definitions: List[DataEventDefinition] = context.get_data_event_definitions()

        if len(time_event_definitions) > 0:
            tep = TimeEventProducer(time_event_definitions)
            tep.subscribe(self)
            tep.start()
        if len(data_event_definitions) > 0:
            dep = DataEventProducer(data_event_definitions)
            dep.subscribe(self)
            dep.start()
        self.account = account
        self.context = context
        self.data_portal = DataPortal(False, "ibTick")

    def __init__(self):
        self.callback_map = {}
        self.event_definitions = []

        self.account = None
        self.data_portal = None

    def get_time_event_definitions(self):
        return [ed for ed in self.event_definitions if isinstance(ed, TimeEventDefinition)]

    def get_data_event_definitions(self):
        return [ed for ed in self.event_definitions if isinstance(ed, DataEventDefinition)]

    def callback_for(self, event_name):
        return self.callback_map[event_name]

    def is_unique_account(self, account_name):
        accounts = AccountModel.objects(name=account_name).all()
        if accounts and len(accounts) >= 0:
            return False
        return True
