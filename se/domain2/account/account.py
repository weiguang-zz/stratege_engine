from abc import *
from enum import Enum
from typing import *

from pandas import Timedelta, Timestamp

from se.domain2.engine.engine import Event, OrderStatusChangeEventDefinition


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


class Bar(object):
    pass


class Tick(object):
    pass


class MatchResult(object):
    def __init__(self, filled_price, filled_quantity, filled_start_time, filled_end_time):
        self.filled_price = filled_price
        self.filled_quantity = filled_quantity
        self.filled_start_time = filled_start_time
        self.filled_end_time = filled_end_time


class Order(metaclass=ABCMeta):

    def __init__(self, code, direction, quantity, place_time):
        self.code = code
        self.direction = direction
        self.quantity = quantity
        self.place_time = place_time
        self.filled_start_time = None
        self.filled_end_time = None
        self.status = OrderStatus.CREATED
        self.filled_quantity = 0
        self.filled_avg_price = 0
        self.fee = 0

    @abstractmethod
    def bar_match(self, bar: Bar) -> MatchResult:
        pass

    @abstractmethod
    def tick_match(self, tick: Tick) -> MatchResult:
        pass

    def order_filled(self, filled_quantity, filled_price, filled_start_time, filled_end_time) -> Event:
        if self.status == OrderStatus.FILLED or self.status == OrderStatus.CANCELED:
            raise RuntimeError("wrong order status")
        self.filled_quantity += filled_quantity
        if self.filled_quantity != self.quantity:
            raise RuntimeError(" wrong filled quantity")
        self.filled_start_time = filled_start_time
        self.filled_end_time = filled_end_time
        self.filled_avg_price = filled_price
        self.status = OrderStatus.FILLED
        return Event(OrderStatusChangeEventDefinition.name, filled_end_time, self)


class MKTOrder(Order):
    def bar_match(self, bar: Bar):
        pass

    def tick_match(self, tick: Tick):
        pass

    def __init__(self, code, direction, quantity, place_time):
        super().__init__(code, direction, quantity, place_time)


class DelayMKTOrder(Order):

    def __init__(self, code, direction, quantity, place_time, delay_time: Timedelta):
        super().__init__(code, direction, quantity, place_time)
        self.delay_time = delay_time

    def bar_match(self, bar: Bar):
        pass

    def tick_match(self, tick: Tick):
        pass


class LimitOrder(Order):

    def __init__(self, code, direction, quantity, place_time, limit_price):
        super().__init__(code, direction, quantity, place_time)
        self.limit_price = limit_price

    def bar_match(self, bar: Bar):
        pass

    def tick_match(self, tick: Tick):
        pass


class CrossMKTOrder(Order):

    def __init__(self, code, direction, quantity, place_time, cross_direction: CrossDirection, cross_price):
        super().__init__(code, direction, quantity, place_time)
        self.cross_direction = cross_direction
        self.cross_price = cross_price

    def bar_match(self, bar: Bar):
        pass

    def tick_match(self, tick: Tick):
        pass


class Operation(object):
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.pnl = 0
        self.orders: List[Order] = []

    def end(self):
        if len(self.orders) <= 0:
            raise RuntimeError("操作没有订单")
        self.end_time = self.orders[-1].filled_end_time
        return Operation()

    def add_order(self, order: Order):
        if len(self.orders) <= 0:
            self.start_time = order.filled_start_time
        self.orders.append(order)

    def get_open_orders(self):
        open_orders = []
        for o in self.orders:
            if o.status == OrderStatus.CREATED:
                open_orders.append(o)
        return open_orders


class AbstractAccount(metaclass=ABCMeta):

    def __init__(self, name: str, initial_cash: float):
        self.name = name
        self.cash = initial_cash
        self.initial_cash = initial_cash
        self.positions = {}
        self.history_net_value = {}
        self.current_operation: Operation = Operation()
        self.history_operations: List[Operation] = []

    @abstractmethod
    def place_order(self, order: Order):
        pass

    def get_open_orders(self):
        return self.current_operation.get_open_orders()

    @abstractmethod
    def match(self, data) -> List[Event]:
        pass


class BacktestAccount(AbstractAccount):
    def match(self, data) -> List[Event]:
        open_orders = self.get_open_orders()
        events = []
        for o in open_orders:
            match_result: MatchResult = None
            if isinstance(data, Bar):
                match_result = o.bar_match(data)
            elif isinstance(data, Tick):
                match_result = o.tick_match(data)
            else:
                raise RuntimeError("wrong data")
            if match_result:
                # change position
                e = o.order_filled(match_result.filled_quantity, match_result.filled_price,
                                   match_result.filled_start_time, match_result.filled_end_time)
                # 修改持仓
                quantity_change = match_result.filled_quantity
                if o.direction == OrderDirection.SELL:
                    quantity_change = -quantity_change
                if o.code in self.positions:
                    self.positions[o.code] += quantity_change
                else:
                    self.positions[o.code] = quantity_change

                if self.positions[o.code] == 0:
                    self.positions.pop(o.code)

                if len(self.positions) <= 0:
                    next_operation = self.current_operation.end()
                    self.history_operations.append(self.current_operation)
                    self.current_operation = next_operation

                events.append(e)

        return events

    def place_order(self, order: Order):
        self.current_operation.add_order(order)
