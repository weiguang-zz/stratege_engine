from abc import ABCMeta, abstractmethod
from typing import List

import pandas as pd
from pandas import Timestamp, Timedelta
from trading_calendars import TradingCalendar

from se.domain.account import AbstractAccount, BacktestAccount, Order, OrderFilledData
from se.domain.data_portal import DataPortal, HistoryDataLoader, CurrentPriceLoader, Bar
from se.domain.event_producer import Event, EventProducer, TimeEventProducer, DateRules, TimeRules, \
    EventType, EventSubscriber


class BarMatchService(EventProducer):

    def start_listen(self, subscriber: EventSubscriber):
        raise NotImplementedError

    def get_events_on_history(self, visible_time_start: Timestamp, visible_time_end: Timestamp):
        if self.freq == Timedelta(minutes=1):
            trading_minutes = self.calendar.minutes_in_range(visible_time_start, visible_time_end)
            opens = self.calendar.session_opens_in_range(visible_time_start, visible_time_end) - Timedelta(minutes=1)
            trading_minutes = trading_minutes.union(pd.DatetimeIndex(opens.values, tz="UTC"))
            trading_minutes = trading_minutes.tz_convert(visible_time_start.tz)
            events: List[Event] = []
            for dt in trading_minutes:
                e = Event(event_type=EventType.TIME, sub_type="match_time", visible_time=dt, data={})
                events.append(e)
            return events
        else:
            opens = self.calendar.session_opens_in_range(visible_time_start, visible_time_end) - Timedelta(minutes=1)
            events: List[Event] = []
            for dt in opens.values:
                e = Event(event_type=EventType.TIME, sub_type="match_time", visible_time=dt, data={})
                events.append(e)
            return events
        pass

    def __init__(self, calendar: TradingCalendar, bar_loader: HistoryDataLoader, freq: Timedelta):
        self.calendar = calendar
        self.bar_loader = bar_loader
        self.freq = freq
        if not (freq == Timedelta(minutes=1) or freq == Timedelta(days=1)):
            raise RuntimeError("只支持分钟或者日粒度的bar")

    def match(self, order: Order, time: Timestamp) -> Event:
        df = self.bar_loader.history_data_in_backtest([order.code], time + self.freq, 1)
        bar_series = df.iloc[0]
        recent_bar: Bar = Bar(bar_series['open'], bar_series['high'], bar_series['low'],
                              bar_series['close'], bar_series['volume'])

        return order.order_type.bar_match(order, recent_bar, time, self.freq)


#
#
# class AbstractMatchService(EventProducer, metaclass=ABCMeta):
#
#     @abstractmethod
#     def match(self, order: Order, time: Timestamp):
#         """
#
#         :param order:
#         :param time: 撮合发生的开始时间
#         :return:
#         """
#         pass
#
#     def start_listen(self, subscriber):
#         raise RuntimeError("不支持的操作")
#         pass
#
#     @abstractmethod
#     def get_events_on_history(self, visible_time_start: Timestamp, visible_time_end: Timestamp):
#         pass
#
#     def __init__(self):
#         self.account = None
#
#     def bind_account(self, account: BacktestAccount):
#         self.account = account
#
#
# class MinBarMatchService(AbstractMatchService):
#     """
#     以分钟的bar作为底层数据的撮合服务， 通过指定calendar和bar_loader, 可以支持不同的交易所
#     """
#
#     def get_events_on_history(self, visible_time_start: Timestamp, visible_time_end: Timestamp):
#         # 获取交易时间段的所有分钟返回
#         trading_minutes = self.calendar.minutes_in_range(visible_time_start, visible_time_end)
#         opens = self.calendar.session_opens_in_range(visible_time_start, visible_time_end) - Timedelta(minutes=1)
#         trading_minutes = trading_minutes.union(pd.DatetimeIndex(opens.values, tz="UTC"))
#         trading_minutes = trading_minutes.tz_convert(visible_time_start.tz)
#         events: List[Event] = []
#         for dt in trading_minutes:
#             e = Event(event_type=EventType.TIME, sub_type="match_time", visible_time=dt, data={})
#             events.append(e)
#         return events
#
#     def match(self, order: Order, time: Timestamp):
#         if not self.account:
#             raise RuntimeError("没有设置账户")
#         if order.place_time.second != 0:
#             raise RuntimeError("下单时间只能是分钟开始")
#         c1: Timestamp = self.calendar.previous_close(time)
#         o1: Timestamp = self.calendar.next_open(c1) - Timedelta(minutes=1)
#         c2: Timestamp = self.calendar.next_close(c1)
#         if c1 < time < o1:
#             raise RuntimeError("撮合时间只能在交易时间段")
#         if time.second != 0:
#             raise RuntimeError("撮合时间只能在分钟的开始")
#
#         # 如果是收盘时间，则以收盘价撮合。 否则则以当前分钟的bar进行撮合
#         one_minute = Timedelta(minutes=1)
#         if time in self.calendar.closes:
#             df: DataFrame = self.bar_loader.history_data_in_backtest([order.code], time, 1)
#             close_price = df.iloc[0]['close']
#             if order.order_type == OrderType.LIMIT and order.limit_price != close_price:
#                 raise RuntimeError("在收盘时下的限价单的限价只能是收盘价")
#             elif order.order_type == OrderType.MKT or \
#                     (order.order_type == OrderType.LIMIT and order.limit_price == close_price):
#                 # 以收盘价撮合
#                 self.account.order_filled(order, order.quantity, close_price, time, time)
#                 return Event(EventType.ACCOUNT, AccountEventType.FILLED, visible_time=time, data={})
#             else:
#                 raise RuntimeError("不支持的订单类型")
#         else:
#             # 以当前的分钟bar进行撮合，不考虑成交量
#             df: DataFrame = self.bar_loader.history_data_in_backtest([order.code], end_time=time + one_minute,
#                                                                      count=1)
#             bar: pd.Series = df.iloc[0]
#             if order.order_type == OrderType.MKT:
#                 # 以开盘价撮合
#                 self.account.order_filled(order, order.quantity, bar['open'], time, time)
#                 return Event(EventType.ACCOUNT, AccountEventType.FILLED, visible_time=time, data={})
#             elif order.order_type == OrderType.LIMIT:
#                 if order.direction == OrderDirection.BUY:
#                     # 比较bar的最高价是否高于限价
#                     if bar['high'] >= order.limit_price:
#                         self.account.order_filled(order, order.quantity, order.limit_price, time, time + one_minute)
#                     return Event(EventType.ACCOUNT, AccountEventType.FILLED, visible_time=time + one_minute, data={})
#                 else:
#                     if bar['low'] <= order.limit_price:
#                         self.account.order_filled(order, order.quantity, order.limit_price, time, time + one_minute)
#                     return Event(EventType.ACCOUNT, AccountEventType.FILLED, visible_time=time + one_minute, data={})
#             elif order.order_type == OrderType.STOP:
#
#
#                 pass
#             else:
#                 raise RuntimeError("不支持的订单类型")
#
#     def __init__(self, calendar: TradingCalendar, bar_loader: HistoryDataLoader):
#         super(MinBarMatchService, self).__init__()
#         self.calendar = calendar
#         self.bar_loader = bar_loader
#         self.opens = (self.calendar.opens - Timedelta(minutes=1)).values
#         self.closes = self.calendar.closes.values
#
#
# class DailyBarMatchService(AbstractMatchService):
#     """
#     以日K数据进行撮合， 这种情况下，只能在每日开盘或者收盘的时候撮合，不能在交易时间段内撮合
#     """
#
#     def match(self, order: Order, time: Timestamp):
#         c1: Timestamp = self.calendar.previous_close(order.place_time)
#         o1: Timestamp = self.calendar.next_open(c1)
#         c2: Timestamp = self.calendar.next_close(c1)
#         if o1 < order.place_time < c2:
#             raise RuntimeError("不允许在交易时间段下单，除非在开盘或者收盘")
#         if time not in self.opens and time not in self.closes:
#             raise RuntimeError("只能在开盘或者收盘的时候撮合")
#
#         if time in self.closes:
#             df: DataFrame = self.bar_loader.history_data_in_backtest([order.code], time, count=1)
#             close_price = df.iloc[0]["close"]
#             if order.order_type == OrderType.LIMIT and order.limit_price != close_price:
#                 raise RuntimeError("收盘只能以收盘价下单")
#             else:
#                 self.account.order_filled(order, order.quantity, close_price, time, time)
#                 return Event(EventType.ACCOUNT, sub_type=AccountEventType.FILLED, visible_time=time, data={})
#         if time in self.opens:
#             df: DataFrame = self.bar_loader.history_data_in_backtest([order.code], time + Timedelta(days=1), count=1)
#             open_price = df.iloc[0]['open']
#             if order.order_type == OrderType.MKT:
#                 self.account.order_filled(order, order.quantity, open_price, time, time)
#                 return Event(EventType.ACCOUNT, sub_type=AccountEventType.FILLED, visible_time=time, data={})
#             elif order.order_type == OrderType.LIMIT:
#                 if order.direction == OrderDirection.BUY:
#                     if df.iloc[0]['high'] >= order.limit_price:
#                         self.account.order_filled(order, order.quantity, order.limit_price, time,
#                                                   self.calendar.next_close(time))
#                         return Event(EventType.ACCOUNT, sub_type=AccountEventType.FILLED,
#                                      visible_time=self.calendar.next_close(time), data={})
#                 else:
#                     if df.iloc[0]['low'] <= order.limit_price:
#                         self.account.order_filled(order, order.quantity, order.limit_price, time,
#                                                   self.calendar.next_close(time))
#                         return Event(EventType.ACCOUNT, sub_type=AccountEventType.FILLED,
#                                      visible_time=self.calendar.next_close(time), data={})
#             else:
#                 raise RuntimeError("不支持的订单类型")
#         return None
#
#     def get_events_on_history(self, visible_time_start: Timestamp, visible_time_end: Timestamp):
#         closes: DatetimeIndex = DatetimeIndex(self.calendar.closes.values)[visible_time_start, visible_time_end]
#         opens: DatetimeIndex = DatetimeIndex((self.calendar.opens - Timedelta(minutes=1)).values)[
#             visible_time_start, visible_time_end]
#         return opens.union(closes)
#
#     def __init__(self, account: BacktestAccount, calendar: TradingCalendar, bar_loader: HistoryDataLoader):
#         self.account = account
#         self.calendar = calendar
#         self.bar_loader = bar_loader
#         self.opens = (self.calendar.opens - Timedelta(minutes=1)).values
#         self.closes = self.calendar.closes.values


class AbstractStrategy(metaclass=ABCMeta):

    @abstractmethod
    def on_event(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
        pass

    def __init__(self, event_producers: List[EventProducer], trading_calendar: TradingCalendar):
        super(AbstractStrategy, self).__init__()
        if len(event_producers) <= 0:
            raise RuntimeError("event producers不能为空")
        self.event_producers = event_producers
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


class StrategyEngine(EventSubscriber):

    def __init__(self, match_service: BarMatchService):
        self.match_service = match_service
        self.event_line: EventLine = EventLine()

    def run_backtest(self, strategy: AbstractStrategy, current_price_loader: CurrentPriceLoader,
                     start: Timestamp, end: Timestamp, initial_cash: float):
        account = BacktestAccount(initial_cash)
        data_portal: DataPortal = DataPortal(backtest_current_price_loader=current_price_loader)

        for ep in strategy.event_producers:
            self.event_line.add_all(ep.get_events_on_history(start, end))
        tep = TimeEventProducer(DateRules.every_day(),
                                TimeRules.market_close(calendar=strategy.trading_calendar, offset=30),
                                sub_type="calc_net_value")
        self.event_line.add_all(tep.get_events_on_history(start, end))
        self.event_line.add_all(self.match_service.get_events_on_history(start, end))

        event: Event = self.event_line.pop_event()
        while event is not None:
            if event.event_type == EventType.ACCOUNT and event.sub_type == "order_filled":
                # 修改账户持仓
                if not isinstance(event.data, OrderFilledData):
                    raise RuntimeError("事件数据类型错误")
                account.order_filled(event.data)

            if self.is_system_event(event):
                self.process_system_event(event, account, data_portal)
            else:
                data_portal.set_current_dt(event.visible_time)
                strategy.on_event(event, account, data_portal)

            event = self.event_line.pop_event()

        return account

    def run(self, strategy: AbstractStrategy, account: AbstractAccount, current_price_loader: CurrentPriceLoader):
        for ep in strategy.event_producers:
            ep.start_listen(self)

        account.start_listen(self)
        self.strategy = strategy
        self.account = account
        self.data_portal = DataPortal(True, realtime_current_price_loader=current_price_loader)

    def on_event(self, event: Event):
        if self.is_system_event(event):
            self.process_system_event(event, self.account, self.data_portal)
        else:
            self.strategy.on_event(event, self.account, self.data_portal)

    @classmethod
    def is_system_event(cls, event):
        if event.event_type == EventType.TIME and \
                (event.sub_type == 'calc_net_value' or event.sub_type == 'match_time'):
            return True
        else:
            return False

    def process_system_event(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
        if event.event_type == EventType.TIME and event.sub_type == 'match_time':
            orders = account.get_open_orders()
            if len(orders) > 0:
                events: List[Event] = []
                for order in orders:
                    order_filled_event: Event = self.match_service.match(order, event.visible_time)
                    if order_filled_event:
                        # 更新账户的持仓
                        events.append(order_filled_event)
                self.event_line.add_all(events)

        elif event.event_type == EventType.TIME and event.sub_type == 'calc_net_value':
            current_prices = {}
            if len(account.get_positions()) > 0:
                current_prices = data_portal.current_price(list(account.get_positions().keys()))
            account.calc_daily_net_value(event.visible_time, current_prices)
