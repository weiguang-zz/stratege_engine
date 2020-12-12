from abc import ABCMeta, abstractmethod

from pandas import Timestamp, Timedelta, DataFrame, DatetimeIndex
from trading_calendars import TradingCalendar

from main.domain.account import AbstractAccount, BacktestAccount, Order, OrderType, OrderDirection
from main.domain.data_portal import DataPortal, TSDataLoader
from main.domain.event_producer import Event, EventProducer, EventBus, TimeEventProducer, DateRules, TimeRules, \
    EventType
from typing import List, Dict
import pandas as pd
from trading_calendars import get_calendar


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

    def get_events_on_history(self, visible_time_start: Timestamp, visible_time_end: Timestamp):
        events = []
        for dt in self.match_times[visible_time_start, visible_time_end]:
            event = Event(event_type=EventType.TIME, sub_type="match_time", visible_time=dt, data={})
            events.append(event)
        return events

    def __init__(self, account: BacktestAccount, price_data: TSDataLoader, match_times: DatetimeIndex):
        self.account = account
        self.price_data = price_data
        self.match_times = match_times


class IBAmericaMinBarMatchService(AbstractMatchService):

    def match(self, order: Order, time: Timestamp):
        if order.place_time.second != 0:
            raise RuntimeError("下单时间只能是分钟开始")
        # 如果是收盘时间，则以收盘价撮合。 否则则以当前分钟的bar进行撮合
        if time not in self.trading_minutes:
            raise RuntimeError("撮合时间错误")
        one_minute = Timedelta(minutes=1)
        if time in self.closes:
            df: DataFrame = self.ts_data_reader.get_ts_data([order.code], start=time, end=time)
            close_price = df.iloc[0]['close']
            if order.order_type == OrderType.LIMIT and order.limit_price != close_price:
                raise RuntimeError("在收盘时下的限价单的限价只能是收盘价")
            else:
                # 以收盘价撮合
                self.account.order_filled(order, order.quantity, close_price, time, time + one_minute)
        else:
            # 以当前的分钟bar进行撮合，不考虑成交量
            df: DataFrame = self.ts_data_reader.get_ts_data([order.code], start=time + one_minute,
                                                            end=time + one_minute)
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


    def __init__(self, start: Timestamp, end: Timestamp, account: BacktestAccount):
        calendar = get_calendar("NYSE")
        trading_minutes = calendar.minutes_in_range(start, end)
        opens = calendar.session_opens_in_range(start, end) - Timedelta(minutes=1)
        trading_minutes = trading_minutes.union(pd.DatetimeIndex(opens.values, tz="UTC"))
        price_data = TSDataLoader(data_provider_name="ibHistory", ts_type_name="ib1MinBar")
        super().__init__(account, price_data, trading_minutes)


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
        tep = TimeEventProducer(DateRules.every_day(), TimeRules.market_close(calendar=strategy.trading_calendar, offset=30),
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
