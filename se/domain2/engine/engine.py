from __future__ import annotations
import logging
import time
from abc import ABCMeta, abstractmethod
from enum import Enum
from threading import Thread
from typing import *

from pandas import DatetimeIndex, Series
from pandas._libs.tslibs.timedeltas import Timedelta
from pandas._libs.tslibs.timestamps import Timestamp
from trading_calendars import TradingCalendar

from se.domain2.account.account import AbstractAccount, BacktestAccount, Bar, Tick, OrderCallback, AccountRepo
from se.domain2.domain import BeanContainer
from se.domain2.time_series.time_series import TimeSeriesRepo, HistoryDataQueryCommand, TimeSeriesSubscriber, TSData, \
    Price


class Rule(metaclass=ABCMeta):

    def is_match(self, calendar: TradingCalendar, dt: Timestamp):
        if not self._next_time:
            self._next_time = self.next_time(calendar, dt)
        if dt >= self._next_time:
            self._next_time = self.next_time(calendar, dt)
            return True
        return False

    @abstractmethod
    def next_time(self, calendar: TradingCalendar, current_time: Timestamp) -> Timestamp:
        pass

    def __init__(self):
        self._next_time = None


class MarketOpen(Rule):
    def next_time(self, calendar: TradingCalendar, current_time: Timestamp) -> Timestamp:
        dt = calendar.next_open(current_time) + Timedelta(minutes=self.offset)
        if dt > current_time:
            return dt
        else:
            return calendar.next_open(calendar.next_open(current_time)) + Timedelta(minutes=self.offset)

    def __init__(self, offset=0):
        # 因为TradingCalendar默认的开盘时间是开盘后一分钟，所以这里做一下调整
        super().__init__()
        self.offset = offset - 1


class MarketClose(Rule):
    def next_time(self, calendar: TradingCalendar, current_time: Timestamp) -> Timestamp:
        dt = calendar.next_close(current_time) + Timedelta(minutes=self.offset)
        if dt > current_time:
            return dt
        else:
            return calendar.next_close(calendar.next_close(current_time)) + Timedelta(minutes=self.offset)

    def __init__(self, offset=0):
        super().__init__()
        self.offset = offset


class Scope(object):
    def __init__(self, codes: List[str], trading_calendar: TradingCalendar):
        self.codes = codes
        self.trading_calendar = trading_calendar


class EventDefinitionType(Enum):
    TIME = 0
    DATA = 1


class BarEventConfig(object):
    def __init__(self, market_open_as_tick: bool = False, market_open_as_tick_delta: Timedelta = Timedelta(seconds=0),
                 bar_open_as_tick: bool = False, bar_open_as_tick_delta: Timedelta = Timedelta(seconds=0),
                 market_close_as_tick=False, market_close_as_tick_delta: Timedelta = Timedelta(seconds=0)):
        if market_open_as_tick and bar_open_as_tick:
            raise RuntimeError("wrong bar event config")
        self.market_open_as_tick = market_open_as_tick
        self.market_close_as_tick = market_close_as_tick
        self.bar_open_as_tick = bar_open_as_tick
        self.market_open_as_tick_delta = market_open_as_tick_delta
        self.market_close_as_tick_delta = market_close_as_tick_delta
        self.bar_open_as_tick_delta = bar_open_as_tick_delta


class EventDataType(Enum):
    BAR = 0
    TICK = 1
    OTHER = 2


class EventDefinition(object):
    def __init__(self, ed_type: EventDefinitionType, time_rule: Rule = None, ts_type_name: str = None,
                 event_data_type: EventDataType = None, bar_config: BarEventConfig = None, order: int = 0):
        self.ed_type = ed_type
        self.time_rule = time_rule
        self.ts_type_name = ts_type_name
        self.order = order
        self.event_data_type = event_data_type
        self.bar_config = bar_config

    def compareTo(self, other: EventDefinition) -> int:
        if self.ed_type == other.ed_type:
            return self.order - other.order
        else:
            if self.ed_type == EventDefinitionType.TIME:
                return 1
            else:
                return -1


class MockedEventProducer(object):

    def start(self, scope: Scope):
        mocked_events = []
        for ed in self.event_definitions:
            mocked_events.extend(self.mocked_event_generator(ed))

        mocked_events = sorted(mocked_events)
        for event in mocked_events:
            self.subscriber.on_event(event)

    def subscribe(self, subscriber: EventSubscriber):
        self.subscriber = subscriber

    def __init__(self, mocked_event_generator: Callable[EventDefinition, List[Event]],
                 event_definitions: List[EventDefinition]):
        self.mocked_event_generator = mocked_event_generator
        self.event_definitions = event_definitions


class EventProducer(TimeSeriesSubscriber):

    def on_data(self, data: TSData):
        ed = self.ts_type_name_to_ed[data.ts_type_name]
        if not ed:
            raise RuntimeError("wrong ts type")
        self.subscriber.on_event(Event(event_definition=ed, visible_time=data.visible_time, data=data))

    def history_events(self, scope: Scope, start: Timestamp, end: Timestamp) -> List[Event]:
        total_events = []

        # 组装时间事件
        if len(self.time_event_definitions) > 0:

            delta = Timedelta(minutes=1)
            p = start
            while p <= end:
                for ed in self.time_event_definitions:
                    if ed.time_rule.is_match(scope.trading_calendar, p):
                        total_events.append(Event(ed, p, {}))

                p += delta

        # 组装数据事件
        if len(self.data_event_definitions) > 0:
            market_opens = DatetimeIndex(scope.trading_calendar.opens.values, tz="UTC") - \
                           Timedelta(minutes=1)
            market_opens = market_opens[(market_opens >= start) & (market_opens <= end)]
            market_closes = DatetimeIndex(scope.trading_calendar.closes.values, tz="UTC")
            market_closes = market_closes[(market_closes >= start) & (market_closes <= end)]

            for ed in self.data_event_definitions:

                ts = BeanContainer.getBean(TimeSeriesRepo).find_one(ed.ts_type_name)
                command = HistoryDataQueryCommand(start, end, scope.codes)
                df = ts.history_data(command, from_local=True)
                for (visible_time, code), values in df.iterrows():
                    data: Dict = values.to_dict()
                    data['visible_time'] = visible_time
                    data['code'] = code

                    if ed.event_data_type == EventDataType.BAR:
                        # 添加bar事件
                        data['start_time'] = data['date']
                        bar = Bar(**data)
                        total_events.append(Event(ed, visible_time, bar))
                        if ed.bar_config.market_open_as_tick and not ed.bar_config.bar_open_as_tick:
                            if bar.start_time in market_opens:
                                total_events.append(Event(ed, bar.start_time + ed.bar_config.market_open_as_tick_delta,
                                                          Tick(code, visible_time, bar.open_price, -1)))

                        if ed.bar_config.bar_open_as_tick:
                            tick_visible_time = bar.start_time + ed.bar_config.bar_open_as_tick_delta
                            total_events.append(Event(ed, tick_visible_time,
                                                      Tick(code, tick_visible_time, bar.open_price, -1)))

                        if ed.bar_config.market_close_as_tick:
                            if bar.visible_time in market_closes:
                                total_events.append(Event(ed, visible_time + ed.bar_config.market_close_as_tick_delta,
                                                          Tick(code, visible_time, bar.close_price, -1)))
                    elif ed.event_data_type == EventDataType.TICK:
                        tick = Tick(**data)
                        total_events.append(Event(ed, tick.visible_time,
                                                  tick))
                    else:
                        total_events.append(Event(ed, visible_time, data))

        return total_events

    def subscribe(self, subscriber: EventSubscriber):
        self.subscriber = subscriber

    def start(self, scope: Scope):
        time_event_definitions = []
        for ed in self.event_definitions:
            if ed.ed_type == EventDefinitionType.TIME:
                # 启动线程来产生时间事件
                time_event_definitions.append(ed)

            else:
                ts = BeanContainer.getBean(TimeSeriesRepo).find_one(ed.ts_type_name)
                ts.subscribe(self, scope.codes)

        TimeEventThread(self.subscriber, time_event_definitions, scope.trading_calendar).start()

    def __init__(self, event_definitions: List[EventDefinition]):
        self.event_definitions = event_definitions
        self.subscriber = None
        self.time_event_definitions = [ed for ed in self.event_definitions if ed.ed_type == EventDefinitionType.TIME]
        self.data_event_definitions = [ed for ed in self.event_definitions if ed.ed_type == EventDefinitionType.DATA]
        self.ts_type_name_to_ed = \
            {ed.ts_type_name: ed for ed in event_definitions if ed.ed_type == EventDefinitionType.DATA}


class Event(object):
    def __init__(self, event_definition: EventDefinition, visible_time: Timestamp, data: object):
        self.event_definition = event_definition
        self.visible_time = visible_time
        self.data = data

    def __lt__(self, other: Event):
        if self.visible_time == other.visible_time:
            if self.event_definition.compareTo(other.event_definition) < 0:
                return True
            else:
                return False
        else:
            return self.visible_time < other.visible_time

    def __str__(self):
        return '[Event]: event_definition:{ed}, visible_time:{visible_time}, data:{data}'. \
            format(ed=self.event_definition, visible_time=self.visible_time, data=self.data)


# class EventDefinition(metaclass=ABCMeta):
#     def __init__(self, name):
#         self.name = name


# class TimeEventDefinition(EventDefinition):
#     def __init__(self, name: str, time_rule: Rule):
#         super().__init__(name)
#         self.time_rule = time_rule


# class DataEventDefinition(EventDefinition):
#     def __init__(self, name: str, ts_type_name: str,
#                  is_bar: bool = False, bar_open_as_tick: bool = False):
#         super().__init__(name)
#         self.ts_type_name = ts_type_name
#         self.is_bar = is_bar
#         self.bar_open_as_tick = bar_open_as_tick


class EventSubscriber(metaclass=ABCMeta):
    @abstractmethod
    def on_event(self, event: Event):
        pass


# class EventProducer(metaclass=ABCMeta):
#     @abstractmethod
#     def history_events(self, scope: Scope, start: Timestamp, end: Timestamp) -> List[Event]:
#         pass
#
#     def subscribe(self, subscriber: EventSubscriber):
#         self.subscriber = subscriber
#
#     @abstractmethod
#     def start(self, scope: Scope):
#         pass
#
#     def __init__(self, event_definitions: List[EventDefinition]):
#         self.event_definitions = event_definitions
#         self.subscriber = None


class TimeEventThread(Thread):
    def __init__(self, subscriber: EventSubscriber, time_event_conditions: List[EventDefinition],
                 calendar: TradingCalendar):
        super().__init__()
        self.name = "time_event_thread"
        self.subscriber = subscriber
        for ed in time_event_conditions:
            if not ed.ed_type == EventDefinitionType.TIME:
                raise RuntimeError("wrong event definition type")
        self.time_event_conditions = time_event_conditions
        self.calendar = calendar

    def run(self) -> None:
        try:
            while True:
                t: Timestamp = Timestamp.now(tz='Asia/Shanghai')
                logging.info("当前时间:{}".format(t))
                for ed in self.time_event_conditions:
                    if ed.time_rule.is_match(self.calendar, t):
                        event = Event(ed, t, {})
                        self.subscriber.on_event(event)
                time.sleep(1)
        except RuntimeError as e:
            logging.error('error', e)


# class TimeEventProducer(EventProducer):
#     def start(self, scope: Scope):
#         # 启动线程来产生时间事件
#         TimeEventThread(self.subscriber, self.event_definitions).start()
#
#     def history_events(self, scope: Scope, start: Timestamp, end: Timestamp):
#         events = []
#         delta = Timedelta(minutes=1)
#         p = start
#         while p <= end:
#             for ed in self.event_definitions:
#                 if not isinstance(ed, TimeEventDefinition):
#                     raise RuntimeError('wrong data')
#                 if ed.time_rule.is_match(scope.trading_calendar, p):
#                     events.append(Event(ed.name, p, {}))
#
#             p += delta
#         return events
#
#     def __init__(self, event_definitions: List[TimeEventDefinition]):
#         for ed in event_definitions:
#             if not isinstance(ed, TimeEventDefinition):
#                 raise RuntimeError("非法的事件定义")
#         super().__init__(event_definitions)
#
#
# class DataEventProducer(EventProducer, TimeSeriesSubscriber):
#     def on_data(self, data: TSData):
#         ed = self.ts_type_name_to_ed[data.ts_type_name]
#         self.subscriber.on_event(Event(name=ed.name, visible_time=data.visible_time, data=data))
#
#     def start(self, scope: Scope):
#         for ed in self.event_definitions:
#             ts = BeanContainer.getBean(TimeSeriesRepo).find_one(ed.name)
#             ts.subscribe(self, scope.codes)
#
#     def history_events(self, scope: Scope, start: Timestamp, end: Timestamp):
#         total_events = []
#         for ed in self.event_definitions:
#             if not isinstance(ed, DataEventDefinition):
#                 raise RuntimeError("wrong event definition")
#             ts = BeanContainer.getBean(TimeSeriesRepo).find_one(ed.ts_type_name)
#             command = HistoryDataQueryCommand(start, end, scope.codes)
#             df = ts.history_data(command, from_local=True)
#             for (visible_time, code), values in df.iterrows():
#                 data = values
#                 if ed.is_bar:
#                     data = Bar(code=code, start_time=values['date'], visible_time=visible_time,
#                                open_price=values['open'], high_price=values['high'], low_price=values['low'],
#                                close_price=values['close'], volume=values['volume'])
#                 total_events.append(Event(ed.name, visible_time, data))
#                 if ed.bar_open_as_tick:
#                     total_events.append((Event(ed.name, values['date'], Tick(code=code, visible_time=values['date'],
#                                                                              price=values['open'], size=-1))))
#         return total_events
#
#     def __init__(self, event_definitions: List[DataEventDefinition]):
#         for ed in event_definitions:
#             if not isinstance(ed, DataEventDefinition):
#                 raise RuntimeError("非法的事件定义")
#         super().__init__(event_definitions)
#         self.ts_type_name_to_ed = {ed.ts_type_name: ed for ed in event_definitions}

class DataPortal(object):

    def history_data(self, ts_type_name, codes, end, window):
        command = HistoryDataQueryCommand(None, end, codes, window)
        ts = BeanContainer.getBean(TimeSeriesRepo).find_one(ts_type_name)
        return ts.history_data(command)

    def __init__(self, is_backtest: bool, ts_type_name_for_current_price: str = None, is_realtime_test: bool = False,
                 mocked_current_prices: Dict = {}):
        if not is_backtest:
            if is_realtime_test and not mocked_current_prices:
                raise RuntimeError("实盘测试的时候，必须指定mocked的当前价格")

            if not is_realtime_test and not ts_type_name_for_current_price:
                raise RuntimeError("need ts_type_name_for_current_price")

        self.ts_type_name_for_current_price = ts_type_name_for_current_price
        self.is_backtest = is_backtest
        self._current_price_map: Mapping[str, Price] = {}
        self.is_realtime_test = is_realtime_test
        self.mocked_current_price = Series(mocked_current_prices)

    def current_price(self, codes: List[str], current_time: Timestamp) -> Mapping[str, Price]:
        """
        在实盘或者回测的时候，获取当前价格的方式不同，实盘的时候，依赖某个时序类型来获取最新的价格。 但是在回测的时候，会从缓存中获取，
        缓存是撮合的时候构建的
        :param current_time:
        :param codes:
        :return:
        """
        if self.is_backtest:
            return {code: self._current_price_map[code] for code in codes}
        else:
            if not self.is_realtime_test:
                ts = BeanContainer.getBean(TimeSeriesRepo).find_one(self.ts_type_name_for_current_price)
                return ts.current_price(codes)
            else:
                cp = self.mocked_current_price[current_time]
                return {code: Price(code, cp[code], current_time) for code in cp.keys() if code in codes}

    def set_current_price(self, code, cp: Price):
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


def calc_net_value(event: Event, account: AbstractAccount, data_portal: DataPortal):
    if len(account.positions) > 0:
        current_price: Mapping[str, Price] = data_portal.current_price(list(account.positions.keys()),
                                                                       event.visible_time)
        cp = {code: current_price[code].price for code in current_price.keys()}
    else:
        cp = {}
    account.calc_net_value(cp, event.visible_time)


class Engine(EventSubscriber):

    def register_event(self, event_definition: EventDefinition,
                       callback: Callable[[Event, AbstractAccount, DataPortal], None]):
        if event_definition in self.callback_map:
            raise RuntimeError("wrong event definition")
        self.callback_map[event_definition] = callback
        self.event_definitions.append(event_definition)

    def on_event(self, event: Event):
        """
        接收实时事件
        :param event:
        :return:
        """
        callback = self.callback_for(event.event_definition)
        callback(event, self.account, self.data_portal)

    def match(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
        if not (isinstance(event.data, Bar) or isinstance(event.data, Tick)):
            raise RuntimeError("wrong event data")
        account.match(event.data)

    def current_price(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
        if isinstance(event.data, Bar):
            data_portal.set_current_price(event.data.code,
                                          Price(event.data.code, event.data.close_price, event.visible_time))
        elif isinstance(event.data, Tick):
            data_portal.set_current_price(event.data.code, Price(event.data.code, event.data.price, event.visible_time))
        else:
            raise RuntimeError("wrong event data")

    def run_backtest(self, strategy: AbstractStrategy, start: Timestamp, end: Timestamp,
                     initial_cash: float,
                     account_name: str):
        # 检查account_name是否唯一
        if not self.is_unique_account(account_name):
            raise RuntimeError("account name重复")
        data_portal = DataPortal(True)
        strategy.initialize(self)
        self.register_event(EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketClose(offset=30)),
                            calc_net_value)
        self.register_event(EventDefinition(ed_type=EventDefinitionType.DATA, ts_type_name="ibMinBar",
                                            event_data_type=EventDataType.BAR,
                                            bar_config=BarEventConfig(bar_open_as_tick=True,
                                                                      bar_open_as_tick_delta=Timedelta(seconds=1),
                                                                      market_close_as_tick=True,
                                                                      market_close_as_tick_delta=Timedelta(seconds=1)
                                                                      ),
                                            order=-10),
                            self.match)
        self.register_event(EventDefinition(ed_type=EventDefinitionType.DATA, ts_type_name="ibMinBar",
                                            event_data_type=EventDataType.BAR,
                                            bar_config=BarEventConfig(market_open_as_tick=True),
                                            order=-100),
                            self.current_price)
        # self.register_event(DataEventDefinition("current_price", "ibMinBar", True, True),
        #                     self.current_price)

        # time_event_definitions: List[TimeEventDefinition] = self.get_time_event_definitions()
        # data_event_definitions: List[DataEventDefinition] = self.get_data_event_definitions()

        event_line = EventLine()
        # if len(data_event_definitions) > 0:
        #     dep = DataEventProducer(data_event_definitions)
        #     event_line.add_all(dep.history_events(strategy.scope, start, end))
        # if len(time_event_definitions) > 0:
        #     tep = TimeEventProducer(time_event_definitions)
        #     event_line.add_all(tep.history_events(strategy.scope, start, end))
        #
        ep = EventProducer(self.event_definitions)
        event_line.add_all(ep.history_events(strategy.scope, start, end))

        account = BacktestAccount(account_name, initial_cash, strategy)
        event: Event = event_line.pop_event()
        while event is not None:
            callback = self.callback_for(event.event_definition)
            try:
                callback(event, account, data_portal)
            except:
                import traceback
                logging.error("{}".format(traceback.format_exc()))

            event = event_line.pop_event()
        # 存储以便后续分析用
        account.save()
        return account

    def run(self, strategy: AbstractStrategy, account: AbstractAccount, is_realtime_test: bool = False,
            mocked_events_generator: Callable[EventDefinition, List[Event]] = None,
            mocked_current_prices: Dict = None):
        if is_realtime_test:
            if not mocked_events_generator or not mocked_current_prices:
                raise RuntimeError("需要mocked_events_generator， mocked_current_prices")

        strategy.initialize(self)
        self.register_event(EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketClose(30)),
                            calc_net_value)
        # time_event_definitions: List[TimeEventDefinition] = self.get_time_event_definitions()
        # data_event_definitions: List[DataEventDefinition] = self.get_data_event_definitions()
        #
        # if len(time_event_definitions) > 0:
        #     tep = TimeEventProducer(time_event_definitions)
        #     tep.subscribe(self)
        #     tep.start(strategy.scope)
        # if len(data_event_definitions) > 0:
        #     dep = DataEventProducer(data_event_definitions)
        #     dep.subscribe(self)
        #     dep.start(strategy.scope)
        self.account = account
        self.data_portal = DataPortal(False, "ibTick", is_realtime_test=is_realtime_test,
                                      mocked_current_prices=mocked_current_prices)
        if not mocked_events_generator:
            ep = EventProducer(self.event_definitions)
            ep.subscribe(self)
            ep.start(strategy.scope)
        else:
            mocked_ep = MockedEventProducer(mocked_events_generator, self.event_definitions)
            mocked_ep.subscribe(self)
            mocked_ep.start(strategy.scope)

    def __init__(self):
        self.callback_map = {}
        self.event_definitions = []

        self.account = None
        self.data_portal = None

    # def get_time_event_definitions(self):
    #     return [ed for ed in self.event_definitions if isinstance(ed, TimeEventDefinition)]
    #
    # def get_data_event_definitions(self):
    #     return [ed for ed in self.event_definitions if isinstance(ed, DataEventDefinition)]

    def callback_for(self, event_definition: EventDefinition):
        return self.callback_map[event_definition]

    def is_unique_account(self, account_name):

        account_repo: AccountRepo = BeanContainer.getBean(AccountRepo)
        if not account_repo.find_one(account_name):
            return True
        else:
            return False
