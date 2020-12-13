from abc import ABCMeta, abstractmethod
from typing import List

import pandas as pd
from pandas import Timestamp, Timedelta, DataFrame, DatetimeIndex
from trading_calendars import TradingCalendar

from main.domain.account import AbstractAccount, BacktestAccount, Order, OrderType, OrderDirection
from main.domain.data_portal import DataPortal, HistoryDataLoader
from main.domain.event_producer import Event, EventProducer, TimeEventProducer, DateRules, TimeRules, \
    EventType, AccountEventType


class AbstractMatchService(EventProducer, metaclass=ABCMeta):

    @abstractmethod
    def match(self, order: Order, time: Timestamp):
        """

        :param order:
        :param time: 撮合发生的开始时间
        :return:
        """
        pass

    def start_listen(self, subscriber):
        raise RuntimeError("不支持的操作")
        pass

    @abstractmethod
    def get_events_on_history(self, visible_time_start: Timestamp, visible_time_end: Timestamp):
        pass


class MinBarMatchService(AbstractMatchService):
    """
    以分钟的bar作为底层数据的撮合服务， 通过指定calendar和bar_loader, 可以支持不同的交易所
    """

    def get_events_on_history(self, visible_time_start: Timestamp, visible_time_end: Timestamp):
        # 获取交易时间段的所有分钟返回
        trading_minutes = self.calendar.minutes_in_range(visible_time_start, visible_time_end)
        opens = self.calendar.session_opens_in_range(visible_time_start, visible_time_end) - Timedelta(minutes=1)
        trading_minutes = trading_minutes.union(pd.DatetimeIndex(opens.values, tz="UTC"))
        return trading_minutes

    def match(self, order: Order, time: Timestamp):
        if order.place_time.second != 0:
            raise RuntimeError("下单时间只能是分钟开始")
        c1: Timestamp = self.calendar.previous_close(time)
        o1: Timestamp = self.calendar.next_open(c1)
        c2: Timestamp = self.calendar.next_close(c1)
        if c1 < time < o1:
            raise RuntimeError("撮合时间只能在交易时间段")
        if time.second != 0:
            raise RuntimeError("撮合时间只能在分钟的开始")

        # 如果是收盘时间，则以收盘价撮合。 否则则以当前分钟的bar进行撮合
        one_minute = Timedelta(minutes=1)
        if time in self.calendar.closes:
            df: DataFrame = self.bar_loader.history_data_in_backtest([order.code], time, 1)
            close_price = df.iloc[0]['close']
            if order.order_type == OrderType.LIMIT and order.limit_price != close_price:
                raise RuntimeError("在收盘时下的限价单的限价只能是收盘价")
            else:
                # 以收盘价撮合
                self.account.order_filled(order, order.quantity, close_price, time, time)
        else:
            # 以当前的分钟bar进行撮合，不考虑成交量
            df: DataFrame = self.bar_loader.history_data_in_backtest([order.code], end_time=time + one_minute,
                                                                     count=1)
            bar: pd.Series = df.iloc[0]
            if order.order_type == OrderType.MKT:
                # 以开盘价撮合
                self.account.order_filled(order, order.quantity, bar['open'], time, time + one_minute)
            elif order.order_type == OrderType.LIMIT:
                if order.direction == OrderDirection.BUY:
                    # 比较bar的最高价是否高于限价
                    if bar['high'] >= order.limit_price:
                        self.account.order_filled(order, order.quantity, order.limit_price, time, time + one_minute)
                else:
                    if bar['low'] <= order.limit_price:
                        self.account.order_filled(order, order.quantity, order.limit_price, time, time + one_minute)

    def __init__(self, account: BacktestAccount, calendar: TradingCalendar, bar_loader: HistoryDataLoader):
        self.account = account
        self.calendar = calendar
        self.bar_loader = bar_loader
        self.opens = (self.calendar.opens - Timedelta(minutes=1)).values
        self.closes = self.calendar.closes.values


class DailyBarMatchService(AbstractMatchService):
    """
    以日K数据进行撮合， 这种情况下，只能在每日开盘或者收盘的时候撮合，不能在交易时间段内撮合
    """

    def match(self, order: Order, time: Timestamp):
        c1: Timestamp = self.calendar.previous_close(order.place_time)
        o1: Timestamp = self.calendar.next_open(c1)
        c2: Timestamp = self.calendar.next_close(c1)
        if o1 < order.place_time < c2:
            raise RuntimeError("不允许在交易时间段下单，除非在开盘或者收盘")
        if time not in self.opens and time not in self.closes:
            raise RuntimeError("只能在开盘或者收盘的时候撮合")

        if time in self.closes:
            df: DataFrame = self.bar_loader.history_data_in_backtest([order.code], time, count=1)
            close_price = df.iloc[0]["close"]
            if order.order_type == OrderType.LIMIT and order.limit_price != close_price:
                raise RuntimeError("收盘只能以收盘价下单")
            else:
                self.account.order_filled(order, order.quantity, close_price, time, time)
                return Event(EventType.ACCOUNT, sub_type=AccountEventType.FILLED, visible_time=time, data={})
        if time in self.opens:
            df: DataFrame = self.bar_loader.history_data_in_backtest([order.code], time + Timedelta(days=1), count=1)
            open_price = df.iloc[0]['open']
            if order.order_type == OrderType.MKT:
                self.account.order_filled(order, order.quantity, open_price, time, time)
                return Event(EventType.ACCOUNT, sub_type=AccountEventType.FILLED, visible_time=time, data={})
            elif order.order_type == OrderType.LIMIT:
                if order.direction == OrderDirection.BUY:
                    if df.iloc[0]['high'] >= order.limit_price:
                        self.account.order_filled(order, order.quantity, order.limit_price, time,
                                                  self.calendar.next_close(time))
                        return Event(EventType.ACCOUNT, sub_type=AccountEventType.FILLED,
                                     visible_time=self.calendar.next_close(time), data={})
                else:
                    if df.iloc[0]['low'] <= order.limit_price:
                        self.account.order_filled(order, order.quantity, order.limit_price, time,
                                                  self.calendar.next_close(time))
                        return Event(EventType.ACCOUNT, sub_type=AccountEventType.FILLED,
                                     visible_time=self.calendar.next_close(time), data={})
        return None

    def get_events_on_history(self, visible_time_start: Timestamp, visible_time_end: Timestamp):
        closes: DatetimeIndex = DatetimeIndex(self.calendar.closes.values)[visible_time_start, visible_time_end]
        opens: DatetimeIndex = DatetimeIndex((self.calendar.opens - Timedelta(minutes=1)).values)[
            visible_time_start, visible_time_end]
        return opens.union(closes)

    def __init__(self, account: BacktestAccount, calendar: TradingCalendar, bar_loader: HistoryDataLoader):
        self.account = account
        self.calendar = calendar
        self.bar_loader = bar_loader
        self.opens = (self.calendar.opens - Timedelta(minutes=1)).values
        self.closes = self.calendar.closes.values


class AbstractStrategy(metaclass=ABCMeta):

    @abstractmethod
    def on_event(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
        pass

    def __init__(self, event_producers: List[EventProducer], trading_calendar: TradingCalendar):
        if len(event_producers) <= 0:
            raise RuntimeError("event producers不能为空")
        self.event_producers = event_producers
        self.trading_calendar = trading_calendar


class EventLine(object):
    def add_all(self, events):
        pass

    def pop_event(self) -> Event:
        pass


class StrategyEngine(object):

    def __init__(self, match_service: AbstractMatchService):
        self.match_service = match_service

    def run_backtest(self, strategy: AbstractStrategy, start: Timestamp, end: Timestamp, initial_cash: float):
        account = BacktestAccount(initial_cash)
        data_portal: DataPortal = DataPortal()
        event_line: EventLine = EventLine()

        for ep in strategy.event_producers:
            event_line.add_all(ep.get_events_on_history(start, end))
        tep = TimeEventProducer(DateRules.every_day(),
                                TimeRules.market_close(calendar=strategy.trading_calendar, offset=30),
                                sub_type="calc_net_value")
        event_line.add_all(tep.get_events_on_history(start, end))
        event_line.add_all(self.match_service.get_events_on_history(start, end))

        event: Event = event_line.pop_event()
        while event is not None:
            if self.is_system_event(event):
                self.process_system_event(event, account, data_portal)
            else:
                strategy.on_event(event, account, data_portal)

            event = event_line.pop_event()

        return account

    def run(self, strategy: AbstractStrategy, account: AbstractAccount):
        for ep in strategy.event_producers:
            ep.start_listen(self)

        account.start_listen(self)
        self.strategy = strategy
        self.account = account
        self.data_portal = DataPortal(True)

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
                for order in orders:
                    self.match_service.match(order, event.visible_time)

        elif event.event_type == EventType.TIME and event.sub_type == 'calc_net_value':
            current_prices = data_portal.current_price("", "", list(account.positions.keys()))
            account.calc_daily_net_value(event.visible_time, current_prices)
