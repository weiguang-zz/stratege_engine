import logging
from abc import *
from enum import Enum
from typing import Dict

from pandas import Timedelta
from pandas import Timestamp

from main.domain.data_portal import Bar
from main.domain.event_producer import EventProducer, Event, EventType


class OrderDirection(Enum):
    BUY = 0
    SELL = 1


class CrossDirection(Enum):
    UP = 0
    DOWN = 1


class OrderStatus(Enum):
    CREATED = 0
    CANCELED = 1
    FILLED = 2


class OrderFilledData(object):
    def __init__(self, order, price: float, quantity: float, start_filled_time: Timestamp, end_filled_time: Timestamp):
        self.order = order
        self.price = price
        self.quantity = quantity
        self.start_filled_time = start_filled_time
        self.end_filled_time = end_filled_time

    def __str__(self):
        return "order:[{}], price:{}, quantity:{}, start_filled_time:{}, end_filled_time:{}".\
            format(self.order, self.price, self.quantity, self.start_filled_time, self.end_filled_time)

class OrderMatchMethod(metaclass=ABCMeta):
    @abstractmethod
    def bar_match(self, order, bar: Bar, time: Timestamp, freq: Timedelta) -> Event:
        pass

    @abstractmethod
    def tick_match(self, order, tick_data: Dict, time: Timestamp) -> Event:
        pass


class MKTOrderMatchMethod(OrderMatchMethod):

    def tick_match(self, order, tick_data: Dict, time: Timestamp):
        raise NotImplementedError

    def bar_match(self, order, bar: Bar, time: Timestamp, freq: Timedelta):
        return Event(EventType.ACCOUNT, "order_filled", time,
                     OrderFilledData(order, bar.open_price, order.quantity, time, time))


class LimitOrderMatchMethod(OrderMatchMethod):

    def bar_match(self, order, bar: Bar, time: Timestamp, freq: Timedelta) -> Event:
        if order.direction == OrderDirection.BUY:
            if bar.low_price <= order.limit_price:
                data = OrderFilledData(order, order.limit_price, order.quantity, time, time + freq)
                return Event(EventType.ACCOUNT, "order_filled", time + freq, data)
        else:
            if bar.high_price >= order.limit_price:
                data = OrderFilledData(order, order.limit_price, order.quantity, time, time + freq)
                return Event(EventType.ACCOUNT, "order_filled", time + freq, data)

        return None

    def tick_match(self, order, tick_data: Dict, time: Timestamp) -> Event:
        raise NotImplementedError


class CrossMKTOrderMatchMethod(OrderMatchMethod):

    def bar_match(self, order, bar: Bar, time: Timestamp, freq: Timedelta) -> Event:
        if order.cross_direction == CrossDirection.UP:
            if bar.high_price >= order.cross_price:
                data = OrderFilledData(order, order.cross_price, order.quantity, time, time + freq)
                return Event(EventType.ACCOUNT, "order_filled", time + freq, data)
        else:
            if bar.low_price <= order.cross_price:
                data = OrderFilledData(order, order.cross_price, order.quantity, time, time + freq)
                return Event(EventType.ACCOUNT, "order_filled", time + freq, data)

        return None

    def tick_match(self, order, tick_data: Dict, time: Timestamp) -> Event:
        raise NotImplementedError


class DelayMKTOrderMatchMethod(OrderMatchMethod):

    def bar_match(self, order, bar: Bar, time: Timestamp, freq: Timedelta) -> Event:
        if time >= (order.place_time + order.delay_time):
            data = OrderFilledData(order, bar.open_price, order.quantity, time, time)
            return Event(EventType.ACCOUNT, "order_filled", time, data)
        return None

    def tick_match(self, order, tick_data: Dict, time: Timestamp) -> Event:
        raise NotImplementedError


class OrderType(Enum):
    MKT = MKTOrderMatchMethod()
    LIMIT = LimitOrderMatchMethod()
    CROSS_MKT = CrossMKTOrderMatchMethod()
    DELAY_MKT = DelayMKTOrderMatchMethod()

    def __init__(self, match_method: OrderMatchMethod):
        self.match_method = match_method

    def bar_match(self, order, bar: Bar, time: Timestamp, freq: Timedelta) -> Event:
        return self.match_method.bar_match(order, bar, time, freq)


class Position(object):

    def __init__(self, code: str, quantity: float):
        self.code = code
        self.quantity = quantity


class Order(object):

    def __init__(self, code: str, quantity: float, order_type: OrderType, direction: OrderDirection, time: Timestamp,
                 limit_price: float = None, cross_price: float = None, cross_direction: CrossDirection = None,
                 delay_time: Timedelta = None):
        if order_type == OrderType.LIMIT and not limit_price:
            raise RuntimeError("limit price必须指定")
        self.code = code
        self.quantity = quantity
        self.order_type = order_type
        self.direction = direction
        self.place_time = time
        self.delay_time = delay_time
        self.start_filled_time = None
        self.end_filled_time = None
        self.filled = 0
        self.limit_price = limit_price
        self.status = OrderStatus.CREATED
        self.cross_price = cross_price
        self.cross_direction = cross_direction

    def __str__(self):
        return "code:{}, order_type:{}, direction:{}, quantity:{}, place_time:{}, delay_time:{}, " \
               "start_filled_time:{}, end_filled_time:{}, filled:{}, limit_price:{}, status:{}, " \
               "cross_price:{}, cross_direction:{}".format(self.code, self.order_type, self.direction,
                                                           self.quantity, self.place_time, self.delay_time,
                                                           self.start_filled_time, self.end_filled_time, self.filled,
                                                           self.limit_price, self.status, self.cross_price,
                                                           self.cross_price)


class AbstractAccount(EventProducer, metaclass=ABCMeta):

    def __init__(self, initial_cash: float):
        super().__init__()
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.positions: Dict[str, Position] = {}
        self.orders = []
        self.events = {}
        self.daily_net_values = {}

    @abstractmethod
    def place_order(self, order):
        pass

    def get_positions(self):
        return self.positions

    def get_open_orders(self):
        open_orders = []
        for order in self.orders:
            if order.status == OrderStatus.CREATED:
                open_orders.append(order)
        return open_orders

    def calc_daily_net_value(self, time: Timestamp, recent_prices: Dict[str, float]):
        daily_net_value = self.cash
        for code in self.positions.keys():
            if code not in recent_prices:
                raise RuntimeError("没有资产的最新价格，无法计算每日净值")
            daily_net_value += self.positions[code].quantity * recent_prices[code]

        self.daily_net_values[time] = daily_net_value


class BacktestAccount(AbstractAccount):

    def start_listen(self, subscriber):
        raise RuntimeError("不支持订阅回测账户")

    def get_events_on_history(self, visible_time_start: Timestamp, visible_time_end: Timestamp):
        raise RuntimeError("不支持的操作")

    def __init__(self, initial_cash: float):
        super(BacktestAccount, self).__init__(initial_cash)

    def place_order(self, order: Order):
        logging.info("下单:" + str(order))
        self.orders.append(order)

    def order_filled(self, order_filled_data: OrderFilledData):
        """
        :return:
        """
        logging.info("订单成交:" + str(order_filled_data))
        if order_filled_data.order.status == OrderStatus.FILLED or order_filled_data.order.status == OrderStatus.CANCELED:
            raise RuntimeError("订单状态非法")

        order_filled_data.order.filled += order_filled_data.quantity
        if order_filled_data.order.filled != order_filled_data.order.quantity:
            raise RuntimeError("订单成交数据错误")
        order_filled_data.order.status = OrderStatus.FILLED
        order_filled_data.order.start_filled_time = order_filled_data.start_filled_time
        order_filled_data.order.end_filled_time = order_filled_data.end_filled_time
        # 扣减现金，增加持仓
        if order_filled_data.order.direction == OrderDirection.SELL:
            quantity = -order_filled_data.quantity
        else:
            quantity = order_filled_data.quantity
        self.cash -= order_filled_data.price * quantity
        if order_filled_data.order.code not in self.positions:
            self.positions[order_filled_data.order.code] = Position(order_filled_data.order.code, quantity)
        else:
            self.positions[order_filled_data.order.code].quantity += quantity
            if self.positions[order_filled_data.order.code].quantity == 0:
                self.positions.pop(order_filled_data.order.code)
