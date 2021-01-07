from abc import ABCMeta, abstractmethod
from typing import *

from pandas import DatetimeIndex
from pandas._libs.tslibs.timedeltas import Timedelta
from pandas._libs.tslibs.timestamps import Timestamp
from pinject import inject
from trading_calendars import TradingCalendar

from se.domain2.account.account import AbstractAccount, Order, Bar, Tick, OrderFilledEvent, BacktestAccount
from se.domain2.time_series.time_series import TimeSeriesRepo, HistoryDataQueryCommand


class EventDefinition(object):

    def __init__(self, name: str):
        self.name = name


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


class TimeEventDefinition(EventDefinition):
    def __init__(self, name: str, date_rule: Rule, time_rule: Rule):
        super().__init__(name)
        self.date_rule = date_rule
        self.time_rule = time_rule


class DataEventDefinition(EventDefinition):
    def __init__(self, name: str, ts_type_name: str):
        super().__init__(name)
        self.ts_type_name = ts_type_name


class OrderStatusChangeEventDefinition(EventDefinition):
    name = "order_status_change"

    def __init__(self):
        super().__init__(self.name)


class Event(object):
    def __init__(self, name: str, visible_time: Timestamp, data: object):
        self.name = name
        self.visible_time = visible_time
        self.data = data


class EventSubscriber(metaclass=ABCMeta):
    @abstractmethod
    def on_event(self, event: Event):
        pass


class EventProducer(metaclass=ABCMeta):
    @abstractmethod
    def history_events(self, start: Timestamp, end: Timestamp) -> List[Event]:
        pass

    @abstractmethod
    def subscribe(self, subscriber: EventSubscriber):
        pass

    def __init__(self, event_definitions: List[EventDefinition]):
        self.event_definitions = event_definitions


class TimeEventProducer(EventProducer):
    def subscribe(self, subscriber: EventSubscriber):
        pass

    def history_events(self, start: Timestamp, end: Timestamp):
        pass

    def __init__(self, event_definitions: List[EventDefinition]):
        for ed in event_definitions:
            if not isinstance(ed, TimeEventDefinition):
                raise RuntimeError("非法的事件定义")
        super().__init__(event_definitions)


class DataEventProducer(EventProducer):
    def history_events(self, start: Timestamp, end: Timestamp):
        pass

    def subscribe(self, subscriber: EventSubscriber):
        pass

    def __init__(self, event_definitions: List[EventDefinition]):
        for ed in event_definitions:
            if not isinstance(ed, DataEventDefinition):
                raise RuntimeError("非法的事件定义")
        super().__init__(event_definitions)


class DataPortal(object):

    def history_data(self, ts_type_name, codes, end, window):
        command = HistoryDataQueryCommand(None, end, codes, window)
        return self.repo.find_one(ts_type_name).history_data(command)

    @inject
    def __init__(self, repo: TimeSeriesRepo):
        self.repo = repo


class EventRegister(object):

    def __init__(self, event_definition: EventDefinition,
                 callback: Callable[[Event, AbstractAccount, DataPortal], None]):
        self.event_definition = event_definition
        self.callback = callback


class MatchService(EventRegister):

    def __init__(self, ts_type_name: str):
        ep = DataEventDefinition("match_data", ts_type_name)

        def callback(event: Event, account: AbstractAccount, data_portal: DataPortal) -> List[Event]:
            if event.name != "match_data":
                raise RuntimeError("wrong event name")
            events = account.match(event.data)

            return events

        super().__init__(ep, callback)


class AbstractStrategy(metaclass=ABCMeta):
    @abstractmethod
    def on_event(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
        pass

    @abstractmethod
    def order_status_change(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
        pass

    def __init__(self, event_registers: List[EventRegister], trading_calendar: TradingCalendar):
        self.event_registers = event_registers
        self.event_registers.append(EventRegister(OrderStatusChangeEventDefinition(), self.order_status_change))
        self.trading_calendar = trading_calendar


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


class Engine(object):

    def calc_net_value(self, event, account, data_portal):
        pass

    @inject
    def __init__(self, data_portal: DataPortal):
        self.data_portal = data_portal
        self.event_registers = []

    def run_backtest(self, strategy: AbstractStrategy, start: Timestamp, end: Timestamp, initial_cash: float,
                     match_service: MatchService):
        er = EventRegister(TimeEventDefinition("calc_net_value",
                                               date_rule=EveryDay(),
                                               time_rule=MarketClose(strategy.trading_calendar, 30)),
                           self.calc_net_value)
        self.event_registers.append(er)
        self.event_registers.append(match_service)

        time_event_definitions: List[TimeEventDefinition] = []
        data_event_definitions: List[DataEventDefinition] = []
        callback_map = {}
        for ep in self.event_registers:
            if isinstance(ep.event_definition, TimeEventDefinition):
                time_event_definitions.append(ep)
            if isinstance(ep.event_definition, DataEventDefinition):
                data_event_definitions.append(ep.event_definition)
            callback_map[ep.event_definition.name] = ep.callback

        event_line = EventLine()
        if len(time_event_definitions) > 0:
            tep = TimeEventProducer(time_event_definitions)
            event_line.add_all(tep.history_events(start, end))
        if len(data_event_definitions) > 0:
            dep = DataEventProducer(data_event_definitions)
            event_line.add_all(dep.history_events(start, end))

        account = BacktestAccount("test_name", initial_cash)
        event: Event = event_line.pop_event()
        while event is not None:
            callback = callback_map[event.name]
            if event.name == "match_data":
                order_status_change_events = callback(event, account, self.data_portal)
                if len(order_status_change_events) > 0:
                    for e in order_status_change_events:
                        strategy.order_status_change(e, account, self.data_portal)
            else:
                callback(event, account, self.data_portal)
            event = event_line.pop_event()

        return account

    def run(self, strategy: AbstractStrategy, account: AbstractAccount):
        pass
