import logging
from abc import *
from enum import Enum
from typing import *

from pandas import Timedelta, Timestamp

from se.infras.models import AccountModel, OperationModel, UserOrderModel


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
    def __init__(self, **kwargs):
        names = ['code', 'start_time', 'visible_time', 'open', 'high', 'low', 'close', 'volume']
        for n in names:
            if n not in kwargs:
                raise RuntimeError("need key:"+ n)

        self.code = kwargs['code']
        self.start_time = kwargs['start_time']
        self.visible_time = kwargs['visible_time']
        self.open_price = kwargs['open']
        self.high_price = kwargs['high']
        self.low_price = kwargs['low']
        self.close_price = kwargs['close']
        self.volume = kwargs['volume']


class Tick(object):
    def __init__(self, code, visible_time, price, size):
        self.code = code
        self.visible_time = visible_time
        self.price = price
        self.size = size


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

    def order_filled(self, filled_quantity, filled_price, filled_start_time, filled_end_time):
        if self.status == OrderStatus.FILLED or self.status == OrderStatus.CANCELED:
            raise RuntimeError("wrong order status")
        self.filled_quantity += filled_quantity
        if self.filled_quantity != self.quantity:
            raise RuntimeError(" wrong filled quantity")
        self.filled_start_time = filled_start_time
        self.filled_end_time = filled_end_time
        self.filled_avg_price = filled_price
        self.status = OrderStatus.FILLED

    def cancel(self):
        self.status = OrderStatus.CANCELED


class MKTOrder(Order):
    def bar_match(self, bar: Bar) -> MatchResult:
        # 以开盘价成交
        return MatchResult(bar.open_price, self.quantity, bar.start_time, bar.visible_time)

    def tick_match(self, tick: Tick) -> MatchResult:
        return MatchResult(tick.price, self.quantity, tick.visible_time, tick.visible_time)

    def __init__(self, code, direction, quantity, place_time):
        super().__init__(code, direction, quantity, place_time)


class DelayMKTOrder(Order):

    def __init__(self, code, direction, quantity, place_time, delay_time: Timedelta):
        super().__init__(code, direction, quantity, place_time)
        self.delay_time = delay_time

    def bar_match(self, bar: Bar):
        if bar.start_time >= (self.place_time + self.delay_time):
            return MatchResult(bar.open_price, self.quantity, bar.start_time, bar.visible_time)
        elif bar.visible_time >= (self.place_time + self.delay_time):
            return MatchResult(bar.close_price, self.quantity, bar.start_time, bar.visible_time)
        return None

    def tick_match(self, tick: Tick):
        if tick.visible_time >= (self.place_time + self.delay_time):
            return MatchResult(tick.price, self.quantity, tick.visible_time, tick.visible_time)
        return None


class LimitOrder(Order):

    def __init__(self, code, direction, quantity, place_time, limit_price):
        super().__init__(code, direction, quantity, place_time)
        self.limit_price = limit_price

    def bar_match(self, bar: Bar):
        if self.direction == OrderDirection.BUY:
            if bar.low_price <= self.limit_price:
                return MatchResult(self.limit_price, self.quantity, bar.start_time, bar.visible_time)
        else:
            if bar.high_price >= self.limit_price:
                return MatchResult(self.limit_price, self.quantity, bar.start_time, bar.visible_time)
        return None

    def tick_match(self, tick: Tick):
        if self.direction == OrderDirection.BUY:
            if tick.price <= self.limit_price:
                return MatchResult(self.limit_price, self.quantity, tick.visible_time, tick.visible_time)
        else:
            if tick.price >= self.limit_price:
                return MatchResult(self.limit_price, self.quantity, tick.visible_time, tick.visible_time)
        return None


class CrossMKTOrder(Order):

    def __init__(self, code, direction, quantity, place_time, cross_direction: CrossDirection, cross_price):
        super().__init__(code, direction, quantity, place_time)
        self.cross_direction = cross_direction
        self.cross_price = cross_price

    def bar_match(self, bar: Bar):
        if self.cross_direction == CrossDirection.UP:
            if bar.high_price >= self.cross_price:
                return MatchResult(self.cross_price, self.quantity, bar.start_time, bar.visible_time)
        else:
            if bar.low_price <= self.cross_price:
                return MatchResult(self.cross_price, self.quantity, bar.start_time, bar.visible_time)
        return None

    def tick_match(self, tick: Tick):
        if self.cross_direction == CrossDirection.UP:
            if tick.price >= self.cross_price:
                return MatchResult(self.cross_price, self.quantity, tick.visible_time, tick.visible_time)
        else:
            if tick.price <= self.cross_price:
                return MatchResult(self.cross_price, self.quantity, tick.visible_time, tick.visible_time)
        return None


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
        self.pnl = self.calc_pnl()
        return Operation()

    def add_order(self, order: Order):
        if len(self.orders) <= 0:
            self.start_time = order.place_time
        self.orders.append(order)

    def get_open_orders(self):
        open_orders = []
        for o in self.orders:
            if o.status == OrderStatus.CREATED:
                open_orders.append(o)
        return open_orders

    def to_model(self):
        order_models = []
        for o in self.orders:
            kwargs = o.__dict__
            kwargs.update({"direction": o.direction.name, "status": o.status.name})
            if isinstance(o, CrossMKTOrder):
                kwargs.update({"cross_direction": o.cross_direction.name if o.cross_direction else None})
            order_models.append(
                UserOrderModel(type=type(o).__name__, **kwargs))
        return OperationModel(start_time=self.start_time,
                              end_time=self.end_time,
                              pnl=self.pnl, orders=order_models)

    def calc_pnl(self):
        pnl = 0
        for od in self.orders:
            if od.status == OrderStatus.FILLED:
                if od.direction == OrderDirection.BUY:
                    pnl -= od.filled_avg_price * od.filled_quantity
                else:
                    pnl += od.filled_avg_price * od.filled_quantity
        return pnl


class OrderCallback(metaclass=ABCMeta):
    @abstractmethod
    def order_status_change(self, order, account):
        pass


class AbstractAccount(metaclass=ABCMeta):

    def __init__(self, name: str, initial_cash: float, order_callback: OrderCallback):
        self.name = name
        self.cash = initial_cash
        self.initial_cash = initial_cash
        self.positions = {}
        self.history_net_value: Mapping[Timestamp, float] = {}
        self.current_operation: Operation = Operation()
        self.history_operations: List[Operation] = []
        self.order_callback = order_callback

    @abstractmethod
    def place_order(self, order: Order):
        pass

    def get_open_orders(self):
        return self.current_operation.get_open_orders()

    @abstractmethod
    def match(self, data):
        pass

    def save(self):
        acc = AccountModel.create(name=self.name, cash=self.cash, initial_cash=self.initial_cash,
                                  positions=self.positions,
                                  history_net_value=self.history_net_value,
                                  current_operation=self.current_operation.to_model(),
                                  history_operations=[operation.to_model() for operation in self.history_operations])
        acc.save()

    def calc_net_value(self, current_price: Mapping[str, float], current_time: Timestamp):
        net_value = 0
        net_value += self.cash
        for code in self.positions.keys():
            net_value += current_price[code] * self.positions[code]
        self.history_net_value[current_time] = net_value

        pass

    def cancel_all_open_orders(self):
        for open_order in self.current_operation.get_open_orders():
            open_order.cancel()
            self.order_callback.order_status_change(open_order, self)


class BacktestAccount(AbstractAccount):
    def match(self, data):
        open_orders = self.get_open_orders()
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
                o.order_filled(match_result.filled_quantity, match_result.filled_price,
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

                self.order_callback.order_status_change(o, self)

    def place_order(self, order: Order):
        self.current_operation.add_order(order)
