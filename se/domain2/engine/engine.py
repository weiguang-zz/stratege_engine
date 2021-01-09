from abc import ABCMeta, abstractmethod
from typing import *

from pandas import DatetimeIndex
from pandas._libs.tslibs.timedeltas import Timedelta
from pandas._libs.tslibs.timestamps import Timestamp
from pinject import inject
from trading_calendars import TradingCalendar

from se.domain2.account.account import AbstractAccount, BacktestAccount, Bar, Tick
from se.domain2.time_series.time_series import TimeSeriesRepo, HistoryDataQueryCommand
from se.infras.models import AccountModel


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


class DataEventDefinition(EventDefinition):
    def __init__(self, name: str, ts_type_name: str):
        super().__init__(name)
        self.ts_type_name = ts_type_name


class OrderStatusChangeEventDefinition(EventDefinition):
    name = "order_status_change"

    def __init__(self):
        super().__init__(self.name)





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


class EventRegister(object):

    def __init__(self, event_definition: EventDefinition,
                 callback: Callable[[Event, AbstractAccount, DataPortal], None]):
        self.event_definition = event_definition
        self.callback = callback


class MatchService(EventRegister):

    def __init__(self, ts_type_name: str):
        ep = DataEventDefinition("match_data", ts_type_name)
        self._current_price: Mapping[str, float] = {}

        def callback(event: Event, account: AbstractAccount, data_portal: DataPortal) -> List[Event]:
            if event.name != "match_data":
                raise RuntimeError("wrong event name")
            if isinstance(event.data, Bar):
                self._current_price[event.data.code] = event.data.close_price
            if isinstance(event.data, Tick):
                self._current_price[event.data.code] = event.data.price
            else:
                raise RuntimeError("wrong event data")

            events = account.match(event.data)

            return events

        super().__init__(ep, callback=callback)

    def current_price(self, codes: List[str]) -> Mapping[str, float]:
        ret = {}
        for code in codes:
            ret[code] = self._current_price[code]
        return ret


class DataPortal(object):

    def history_data(self, ts_type_name, codes, end, window):
        command = HistoryDataQueryCommand(None, end, codes, window)
        ts = TimeSeriesRepo.find_one(ts_type_name)
        return ts.history_data(command)

    def __init__(self, is_backtest: bool, match_service: MatchService, ts_type_name_for_current_price: str):
        if is_backtest:
            if not match_service:
                raise RuntimeError("need match_service")
        else:
            if not ts_type_name_for_current_price:
                raise RuntimeError("need ts_type_name_for_current_price")

        self.ts_type_name_for_current_price = ts_type_name_for_current_price
        self.is_backtest = is_backtest
        self.match_service = match_service

    def current_price(self, codes: List[str]):
        """
        在实盘或者回测的时候，获取当前价格的方式不同，实盘的时候，依赖某个时序类型来获取最新的价格。 但是在回测的时候，则会从撮合服务
        中获取
        :param codes:
        :return:
        """
        if self.is_backtest:
            return self.match_service.current_price(codes)
        else:
            ts = TimeSeriesRepo.find_one(self.ts_type_name_for_current_price)
            return ts.current_price(codes)


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

    def calc_net_value(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
        if len(account.positions) > 0:
            current_price: Mapping[str, float] = self.data_portal.current_price(list(account.positions.keys()))
            account.calc_net_value(current_price)

    @inject
    def __init__(self, data_portal: DataPortal, match_service: MatchService):
        self.data_portal = data_portal
        self.event_registers = []
        self.match_service = match_service

        self.event_registers.append(match_service)

    def run_backtest(self, strategy: AbstractStrategy, start: Timestamp, end: Timestamp, initial_cash: float,
                     account_name: str):
        # 检查test_id是否唯一
        if not self.is_unique_account(account_name):
            raise RuntimeError("account name重复")
        calc_net_value_callback = EventRegister(TimeEventDefinition("calc_net_value",
                                                                    date_rule=EveryDay(),
                                                                    time_rule=MarketClose(strategy.trading_calendar,
                                                                                          30)),
                                                self.calc_net_value)
        self.event_registers.append(calc_net_value_callback)

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

        account = BacktestAccount(account_name, initial_cash)
        event: Event = event_line.pop_event()
        while event is not None:
            callback = callback_map[event.name]
            if event.name == "match_data":
                order_status_change_events: List[Event] = callback(event, account, self.data_portal)
                if len(order_status_change_events) > 0:
                    for e in order_status_change_events:
                        strategy.order_status_change(e, account, self.data_portal)
            else:
                callback(event, account, self.data_portal)
            event = event_line.pop_event()
        # 存储以便后续分析用
        account.save()
        return account

    def run(self, strategy: AbstractStrategy, account: AbstractAccount):
        pass

    def is_unique_account(self, account_name):
        accounts = AccountModel.objects(name=account_name).all()
        if accounts and len(accounts) >= 0:
            return False
        return True
