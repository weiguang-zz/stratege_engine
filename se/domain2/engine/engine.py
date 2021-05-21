from __future__ import annotations
import logging
import threading
import time
from abc import ABCMeta, abstractmethod
from enum import Enum
from threading import Thread
from typing import *

import pandas as pd
import trading_calendars
from pandas import DatetimeIndex, Series
from pandas._libs.tslibs.timedeltas import Timedelta
from pandas._libs.tslibs.timestamps import Timestamp
from trading_calendars import TradingCalendar

from se.domain2.account.account import AbstractAccount, BacktestAccount, Bar, Tick, OrderCallback, AccountRepo, \
    LimitOrder, OrderStatus, MKTOrder, OrderDirection
from se.domain2.domain import BeanContainer
from se.domain2.monitor import alarm, AlarmLevel, EscapeParam, do_alarm, retry, do_log
from se.domain2.time_series.time_series import TimeSeriesRepo, HistoryDataQueryCommand, TimeSeriesSubscriber, TSData, \
    Price, TimeSeries
import numpy as np


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

    def __init__(self, minute_offset, second_offset):
        self._next_time = None
        self.minute_offset = minute_offset
        self.second_offset = second_offset


class MarketOpen(Rule):
    def next_time(self, calendar: TradingCalendar, current_time: Timestamp) -> Timestamp:
        # 因为TradingCalendar默认的开盘时间是开盘后一分钟，所以这里做一下调整
        dt = calendar.next_open(current_time) + Timedelta(minutes=self.minute_offset - 1) + \
             Timedelta(seconds=self.second_offset)
        # 因为经过offset调整之后，这个时间可能在当前时间之前，比如如果minute_offset=-30, 当前时间为盘前10分钟，则下一个执行时间
        # 是下下个开盘前的10分钟
        if dt <= current_time:
            dt = calendar.next_open(calendar.next_open(current_time)) + Timedelta(minutes=self.minute_offset - 1) \
                 + Timedelta(seconds=self.second_offset)
        return dt

    def __init__(self, minute_offset=0, second_offset=0):
        super().__init__(minute_offset, second_offset)


class MarketClose(Rule):
    def next_time(self, calendar: TradingCalendar, current_time: Timestamp) -> Timestamp:
        dt = calendar.next_close(current_time) + Timedelta(minutes=self.minute_offset) + \
             Timedelta(seconds=self.second_offset)
        if dt <= current_time:
            dt = calendar.next_close(calendar.next_close(current_time)) + Timedelta(minutes=self.minute_offset) + \
                 Timedelta(seconds=self.second_offset)
        return dt

    def __init__(self, minute_offset=0, second_offset=0):
        super().__init__(minute_offset, second_offset)


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
                    if ed.time_rule.second_offset != 0:
                        raise RuntimeError("回测过程中的时间事件不允许秒级偏移")
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
                command.with_calendar(scope.trading_calendar)
                df = ts.history_data(command, from_local=True)
                for (visible_time, code), values in df.iterrows():
                    data: Dict = values.to_dict()
                    if ed.event_data_type == EventDataType.BAR:
                        # 添加bar事件
                        if "start_time" in data:
                            start_time = data['start_time']
                        else:
                            start_time = data['date']

                        bar = Bar(ed.ts_type_name, visible_time, code, start_time, data['open'], data['high'],
                                  data['low'], data['close'], data['volume'])
                        total_events.append(Event(ed, visible_time, bar))
                        if ed.bar_config.market_open_as_tick and not ed.bar_config.bar_open_as_tick:
                            if bar.start_time in market_opens:
                                total_events.append(Event(ed, bar.start_time + ed.bar_config.market_open_as_tick_delta,
                                                          Tick(ed.ts_type_name, visible_time, code,
                                                               bar.open_price, -1)))

                        if ed.bar_config.bar_open_as_tick:
                            tick_visible_time = bar.start_time + ed.bar_config.bar_open_as_tick_delta
                            total_events.append(Event(ed, tick_visible_time,
                                                      Tick(ed.ts_type_name, tick_visible_time, code,
                                                           bar.open_price, -1)))

                        if ed.bar_config.market_close_as_tick:
                            if bar.visible_time in market_closes:
                                total_events.append(Event(ed, visible_time + ed.bar_config.market_close_as_tick_delta,
                                                          Tick(ed.ts_type_name, visible_time, code,
                                                               bar.close_price, -1)))
                    elif ed.event_data_type == EventDataType.TICK:
                        tick = Tick(ed.ts_type_name, visible_time, code, data['price'], data['size'])
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


class EventSubscriber(metaclass=ABCMeta):
    @abstractmethod
    def on_event(self, event: Event):
        pass


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
        while True:
            try:
                t: Timestamp = Timestamp.now(tz='Asia/Shanghai')
                logging.info("当前时间:{}".format(t))
                for ed in self.time_event_conditions:
                    if ed.time_rule.is_match(self.calendar, t):
                        event = Event(ed, t, {})
                        self.subscriber.on_event(event)
                time.sleep(1)
            except:
                import traceback
                logging.error("{}".format(traceback.format_exc()))


class DataPortal(TimeSeriesSubscriber):

    def on_data(self, data: TSData):
        pass
        # if isinstance(data, Bar):
        #     self._current_price_map[data.code] = Price(data.code, data.close_price, data.visible_time)
        # elif isinstance(data, Tick):
        #     self._current_price_map[data.code] = Price(data.code, data.price, data.visible_time)
        # else:
        #     raise RuntimeError(" wrong ts_data type")

    def history_data(self, ts_type_name, command: HistoryDataQueryCommand):
        ts = BeanContainer.getBean(TimeSeriesRepo).find_one(ts_type_name)
        return ts.history_data(command)

    def __init__(self, is_backtest: bool, ts_type_name_for_current_price: str, engine: Engine = None,
                 subscribe_codes: List[str] = None, mocked_current_prices: Dict[Timestamp, Dict[str, Price]] = {}):
        self.ts_type_name_for_current_price = ts_type_name_for_current_price
        self.is_backtest = is_backtest
        self._current_price_map: Mapping[str, Price] = {}
        self.mocked_current_price = mocked_current_prices
        self.subscribe_codes = subscribe_codes
        if not is_backtest:
            if not subscribe_codes:
                raise RuntimeError("need subscribe codes")
            ts_repo: TimeSeriesRepo = BeanContainer.getBean(TimeSeriesRepo)
            self.realtime_ts: TimeSeries = ts_repo.find_one(ts_type_name_for_current_price)
            self.realtime_ts.subscribe(self, subscribe_codes)
            self.start_check_realtime_data_thread()
        else:
            if not engine:
                raise RuntimeError("need engine")
            engine.register_event(EventDefinition(ed_type=EventDefinitionType.DATA,
                                                  ts_type_name=ts_type_name_for_current_price,
                                                  event_data_type=EventDataType.BAR,
                                                  bar_config=BarEventConfig(market_open_as_tick=True),
                                                  order=-100),
                                  self.set_current_price)

    def start_check_realtime_data_thread(self):
        def do_check(code: str):
            if 'USD' in code:
                # 如果是盘前，允许有30分钟的延迟
                # 如果是盘中，允许有1分钟的延迟
                now = Timestamp.now(tz='Asia/Shanghai')
                us_calendar: TradingCalendar = trading_calendars.get_calendar("NYSE")
                pre_open_last = Timedelta(minutes=30, hours=5)
                pre_open_allowed_delay = Timedelta(minutes=30)
                market_open_allowed_delay = Timedelta(minutes=1)

                next_close = us_calendar.next_close(now)
                previous_open = us_calendar.previous_open(next_close)
                pre_open_start = previous_open - pre_open_last
                if pre_open_start + pre_open_allowed_delay < now < previous_open:
                    try:
                        self.current_price([code], now, pre_open_allowed_delay)
                    except:
                        import traceback
                        logging.error("{}".format(traceback.format_exc()))
                elif previous_open < now < next_close:
                    try:
                        self.current_price([code], now, market_open_allowed_delay)
                    except:
                        import traceback
                        logging.error("{}".format(traceback.format_exc()))

        def check_realtime_data():
            while True:
                try:
                    logging.info("开始检查实时数据")
                    for code in self.subscribe_codes:
                        do_check(code)
                except:
                    import traceback
                    err_msg = "检查实时数据异常:{}".format(traceback.format_exc())
                    logging.error(err_msg)
                time.sleep(10 * 60)

        threading.Thread(name="check_realtime_data", target=check_realtime_data).start()

    @alarm(level=AlarmLevel.ERROR, target="获取最新价格")
    @retry(limit=10, interval=1)
    def current_price(self, codes: List[str], current_time: Timestamp,
                      delay_allowed: Timedelta = Timedelta(seconds=10)) -> Mapping[str, Price]:
        if self.mocked_current_price:
            cp = self.mocked_current_price[current_time]
            return {code: cp[code] for code in codes}
        else:
            # 如果是回测，从本地缓存中获取
            # 如果是实盘，从时序数据获取
            ret = {}
            if self.is_backtest:
                for code in codes:
                    if code in self._current_price_map:
                        ret[code] = self._current_price_map[code]
            else:
                ret = self.realtime_ts.func.current_price(codes)
            # 检查数据是否缺失或者有延迟
            for code in codes:
                if code not in ret:
                    raise RuntimeError("没有获取到{}的最新价格，当前时间:{}".format(code, current_time))
                price = ret[code]
                if delay_allowed:
                    if current_time - price.time > delay_allowed:
                        raise RuntimeError("没有获取到{}的最新价格，当前时间:{}, 最新价格:{}, 允许的延迟:{}".
                                           format(code, current_time, price.__dict__, delay_allowed))
            return ret

    def set_current_price(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
        if isinstance(event.data, Bar):
            self._current_price_map[event.data.code] = \
                Price(event.data.code, event.data.close_price, event.visible_time)
        elif isinstance(event.data, Tick):
            self._current_price_map[event.data.code] = Price(event.data.code, event.data.price, event.visible_time)
        else:
            raise RuntimeError("wrong event data")

    @alarm(level=AlarmLevel.ERROR, target="获取最新的买卖价")
    @retry(limit=10, interval=1)
    def current_bid_ask(self, codes: List[str], delay_allowed: Timedelta = Timedelta(seconds=10)) -> \
            Mapping[str, BidAsk]:
        if self.is_backtest:
            raise RuntimeError("回测中不支持")
        now = Timestamp.now(tz='Asia/Shanghai')
        ret = self.realtime_ts.func.current_bid_ask(codes)
        # 检查数据是否缺失或者有延迟
        for code in codes:
            if code not in ret:
                raise RuntimeError("没有获取到{}的最新买卖价，当前时间:{}".format(code, now))
            bid_ask: BidAsk = ret[code]
            if not (bid_ask.bid_size and bid_ask.bid_price and bid_ask.ask_price and bid_ask.ask_size):
                raise RuntimeError("买卖报价缺少数据")
            if delay_allowed:
                if now - bid_ask.update_time > delay_allowed:
                    raise RuntimeError("没有获取到{}的最新买卖价，当前时间:{}, 最新买卖价:{}, 允许的延迟:{}".
                                       format(code, now, bid_ask.__dict__, delay_allowed))
        return ret

class BidAsk(object):
    def __init__(self):
        self.update_time = None
        self.bid_size = None
        self.bid_price = None
        self.ask_size = None
        self.ask_price = None

    def with_ask_price(self, ask_price):
        self.ask_price = ask_price
        self.update_time = Timestamp.now(tz='Asia/Shanghai')

    def with_ask_size(self, ask_size):
        self.ask_size = ask_size
        self.update_time = Timestamp.now(tz='Asia/Shanghai')

    def with_bid_price(self, bid_price):
        self.bid_price = bid_price
        self.update_time = Timestamp.now(tz='Asia/Shanghai')

    def with_bid_size(self, bid_size):
        self.bid_size = bid_size
        self.update_time = Timestamp.now(tz='Asia/Shanghai')


class AbstractStrategy(OrderCallback, metaclass=ABCMeta):

    # @alarm(target="订单状态变更", escape_params=[EscapeParam(index=0, key='self'), EscapeParam(index=2, key='account')])
    @do_log(target_name="订单状态变更", escape_params=[EscapeParam(index=0, key='self'), EscapeParam(index=2, key='account')])
    def order_status_change(self, order, account):
        self.do_order_status_change(order, account)

    def ensure_order_filled_v2(self, account: AbstractAccount, data_portal: DataPortal, order: LimitOrder,
                               duration: int, delta=0.01):
        """
        在duration所指定的时间范围内，跟踪市场上的买一和卖一的价格。 超过duration之后，则挂市价单
        :param delta:
        :param account:
        :param data_portal:
        :param order:
        :param duration:
        :return:
        """
        def do_ensure():
            try:
                now = Timestamp.now(tz='Asia/Shanghai')
                threshold = now + Timedelta(seconds=duration)
                while now <= threshold:
                    if (order.status == OrderStatus.CANCELED) or (order.status == OrderStatus.FILLED) or order.status == OrderStatus.FAILED:
                        logging.info("没有为成交的订单，不需要ensure")
                        break
                    bid_ask = None
                    try:
                        bid_ask: BidAsk = data_portal.current_bid_ask([order.code])[order.code]
                    except:
                        pass
                    if bid_ask:
                        if order.direction == OrderDirection.BUY:
                            target_price = bid_ask.bid_price + delta
                            # 乘以1.1是为了防止跟自己的出价进行比较
                            if abs(target_price - order.limit_price) > delta * 1.1:
                                order.limit_price = target_price
                                update_reason = "当前时间:{}, 订单的限价:{}, 最新的买卖价:{}, 设定的delta:{}".\
                                    format(now, order.limit_price, bid_ask.__dict__, delta)
                                account.update_order(order, update_reason)
                        else:
                            target_price = bid_ask.ask_price - delta
                            if abs(target_price - order.limit_price) > delta * 1.1:
                                order.limit_price = target_price
                                update_reason = "当前时间:{}, 订单的限价:{}, 最新的买卖价:{}, 设定的delta:{}". \
                                    format(now, order.limit_price, bid_ask.__dict__, delta)
                                new_order = account.update_order(order, update_reason)
                                if new_order:
                                    # 针对td的情况，修改订单意味着取消原来的订单，并且创建一个新的订单
                                    # 这个时候需要启动监听线程
                                    new_duration = (threshold - now).seconds
                                    self.ensure_order_filled_v2(account, data_portal, new_order, new_duration, delta)
                                    pass
                    time.sleep(0.1)
                    now = Timestamp.now(tz='Asia/Shanghai')
                if order.status == OrderStatus.CREATED or order.status == OrderStatus.SUBMITTED or \
                        order.status == OrderStatus.PARTIAL_FILLED:
                    logging.info("订单在规定时间内没有成交，将会使用市价单挂单")
                    account.cancel_open_order(order)
                    new_order = MKTOrder(order.code, order.direction, int(order.quantity - order.filled_quantity), now)
                    account.place_order(new_order)
                    # 一般情况下，市价单会立即成交，但是存在某个资产不可卖空的情况，这种情况下，市价单可能会被保留
                    # 针对这种情况，需要取消掉，以免在不合适的时候成交
                    time.sleep(10)
                    if new_order.status == OrderStatus.SUBMITTED or new_order.status == OrderStatus.PARTIAL_FILLED:
                        logging.info("市价单没有在规定的时间内成交，猜想可能是因为没有资产不可卖空导致的，将会取消订单")
                        account.cancel_open_order(new_order)
            except:
                import traceback
                err_msg = "ensure order filled失败:{}".format(traceback.format_exc())
                logging.error(err_msg)
                # 显示告警，因为在线程的Runable方法中，不能再抛出异常。
                do_alarm('ensure order filled', AlarmLevel.ERROR, None, None, '{}'.format(traceback.format_exc()))

        if not isinstance(order, LimitOrder):
            raise RuntimeError("wrong order type")

        threading.Thread(name='ensure_order_filled', target=do_ensure).start()

    def ensure_order_filled(self, account: AbstractAccount, data_portal: DataPortal, order: LimitOrder, period: int,
                            retry_count: int):
        """
        将通过如下算法来保证订单一定成交：
        每隔一定时间检查订单是否还未成交，如果是的话，则取消原来的订单，并且按照最新的价格下一个新的订单，同时监听新的订单
        :param account:
        :param order:
        :param period:
        :param retry_count:
        :return:
        """
        def ensure():
            try:
                time.sleep(period)
                logging.info("ensure order filled thread start")
                if order.status == OrderStatus.CREATED or order.status == OrderStatus.PARTIAL_FILLED \
                        or order.status == OrderStatus.SUBMITTED:
                    account.cancel_open_order(order)
                    # 保证下单的总价值是相同的
                    remain_quantity = order.quantity - order.filled_quantity
                    cp = data_portal.current_price([order.code], Timestamp.now(tz='Asia/Shanghai'))[order.code]
                    new_quantity = int(remain_quantity * order.limit_price / cp.price)
                    now = Timestamp.now(tz='Asia/Shanghai')
                    if retry_count > 1:
                        new_order = LimitOrder(order.code, order.direction, new_quantity,
                                               now, cp.price)
                    else:
                        new_order = MKTOrder(order.code, order.direction, new_quantity, now)
                    account.place_order(new_order)
                    if retry_count > 1:
                        self.ensure_order_filled(account, data_portal, new_order, period, retry_count - 1)
                else:
                    logging.info("没有还未成交的订单, 不需要ensure")
            except:
                import traceback
                err_msg = "ensure order filled失败:{}".format(traceback.format_exc())
                logging.error(err_msg)
                # 显示告警，因为在线程的Runable方法中，不能再抛出异常。
                do_alarm('ensure order filled', AlarmLevel.ERROR, None, None, '{}'.format(traceback.format_exc()))

        if retry_count <= 0:
            raise RuntimeError('wrong retry count')
        if not isinstance(order, LimitOrder):
            raise RuntimeError("wrong order type")

        threading.Thread(name='ensure_order_filled', target=ensure).start()

    @abstractmethod
    def do_order_status_change(self, order, account):
        pass

    @abstractmethod
    def do_initialize(self, engine: Engine, data_portal: DataPortal):
        pass

    def initialize(self, engine: Engine, data_portal: DataPortal):
        self.is_backtest = engine.is_backtest
        self.do_initialize(engine, data_portal)

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
                                                                       event.visible_time,
                                                                       delay_allowed=Timedelta(seconds=20))
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

    def run_backtest(self, strategy: AbstractStrategy, start: Timestamp, end: Timestamp,
                     initial_cash: float,
                     account_name: str,
                     ts_type_name_for_match: str):
        self.is_backtest = True
        # 检查account_name是否唯一
        if not self.is_unique_account(account_name):
            raise RuntimeError("account name重复")
        data_portal = DataPortal(True, ts_type_name_for_match, self)
        strategy.initialize(self, data_portal)
        self.register_event(EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketClose(), order=1000),
                            calc_net_value)
        self.register_event(EventDefinition(ed_type=EventDefinitionType.DATA, ts_type_name=ts_type_name_for_match,
                                            event_data_type=EventDataType.BAR,
                                            bar_config=BarEventConfig(bar_open_as_tick=True,
                                                                      bar_open_as_tick_delta=Timedelta(seconds=1),
                                                                      market_close_as_tick=True,
                                                                      market_close_as_tick_delta=Timedelta(seconds=1)
                                                                      ),
                                            order=-10),
                            self.match)

        event_line = EventLine()

        ep = EventProducer(self.event_definitions)
        event_line.add_all(ep.history_events(strategy.scope, start, end))

        account = BacktestAccount(account_name, initial_cash).with_order_callback(strategy)
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
        self.is_backtest = False
        if is_realtime_test:
            if not mocked_events_generator or not mocked_current_prices:
                raise RuntimeError("需要mocked_events_generator， mocked_current_prices")

        self.register_event(EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketClose(second_offset=10)),
                            calc_net_value)
        self.account = account
        self.data_portal = DataPortal(False, "ibMarketData", subscribe_codes=strategy.scope.codes,
                                      mocked_current_prices=mocked_current_prices)
        strategy.initialize(self, self.data_portal)
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
        self.is_backtest = False

    def callback_for(self, event_definition: EventDefinition):
        return self.callback_map[event_definition]

    def is_unique_account(self, account_name):

        account_repo: AccountRepo = BeanContainer.getBean(AccountRepo)
        if not account_repo.find_one(account_name):
            return True
        else:
            return False
