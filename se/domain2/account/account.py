from __future__ import annotations
import logging
import uuid
from abc import *
from enum import Enum
from typing import *

from pandas import Timedelta, Timestamp

from se.domain2.domain import BeanContainer
from se.domain2.monitor import alarm, AlarmLevel, EscapeParam, do_log
from se.domain2.time_series.time_series import Bar, Tick


class OrderDirection(Enum):
    BUY = "BUY"
    SELL = "SELL"


class CrossDirection(Enum):
    UP = "UP"
    DOWN = "DOWN"


class OrderStatus(Enum):
    CREATED = "CREATED"
    SUBMITTED = "SUBMITTED"
    CANCELED = "CANCELED"
    FILLED = "FILLED"
    FAILED = "FAILED"
    PARTIAL_FILLED = "PARTIAL_FILLED"


class OrderExecution(object):
    def __init__(self, id: str, version: int, commission: float, filled_quantity: float, filled_avg_price: float,
                 filled_start_time: Timestamp, filled_end_time: Timestamp, direction: OrderDirection,
                 attributes: Dict[str, str] = None):
        self.id = id
        self.version = version
        self.commission = commission
        self.filled_quantity = filled_quantity
        self.filled_avg_price = filled_avg_price
        self.filled_start_time = filled_start_time
        self.filled_end_time = filled_end_time
        self.direction = direction
        self.attributes = attributes

    def cash_change(self):
        cash_change = 0
        cash_change -= self.commission
        if self.direction == OrderDirection.BUY:
            cash_change = cash_change - self.filled_quantity * self.filled_avg_price
        else:
            cash_change = cash_change + self.filled_quantity * self.filled_avg_price
        return cash_change


class Order(metaclass=ABCMeta):

    def __init__(self, code, direction, quantity, place_time, quantity_split: List[int] = None):
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
        self.execution_map = {}
        # 没有用的字段
        self.quantity_split = quantity_split
        self.ib_order_id = None
        self.td_order_id = None
        self.reason = None
        self.update_reasons = []

    def with_reason(self, reason: str):
        self.reason = reason
        return self

    @abstractmethod
    def bar_match(self, bar: Bar) -> OrderExecution:
        pass

    @abstractmethod
    def tick_match(self, tick: Tick) -> OrderExecution:
        pass

    def order_filled(self, execution: OrderExecution):
        if execution.id in self.execution_map:
            old_execution: OrderExecution = self.execution_map[execution.id]
            if execution.version > old_execution.version:
                logging.info("订单执行被修订，老的执行详情:{}, 新的执行详情:{}".
                             format(old_execution.__dict__, execution.__dict__))
                self.reverse(old_execution)
                self._order_filled(execution)
        else:
            self._order_filled(execution)
        if self.filled_quantity > self.quantity or (self.filled_quantity < 0):
            raise RuntimeError("wrong filled quantity")
        elif self.filled_quantity == self.quantity:
            if not self.filled_start_time:
                self.filled_start_time = execution.filled_start_time
            self.filled_end_time = execution.filled_end_time
            self.status = OrderStatus.FILLED
        elif self.filled_quantity > 0:
            self.status = OrderStatus.PARTIAL_FILLED
            if not self.filled_start_time:
                self.filled_start_time = execution.filled_start_time
        else:
            # self.filled_quantity = 0
            logging.warning("订单执行没有修改订单成交数量，订单执行:{}，订单:{}".format(execution.__dict__, self.__dict__))

    def _order_filled(self, execution: OrderExecution):

        self.filled_avg_price = (execution.filled_avg_price * execution.filled_quantity +
                                 self.filled_quantity * self.filled_avg_price) / \
                                (self.filled_quantity + execution.filled_quantity)
        self.filled_quantity += execution.filled_quantity
        # if self.filled_quantity > self.quantity:
        #     raise RuntimeError("wrong filled quantity")
        # if self.filled_quantity == self.quantity:
        #     self.filled_end_time = execution.filled_end_time
        #     self.status = OrderStatus.FILLED
        self.fee += execution.commission
        self.execution_map[execution.id] = execution

    def reverse(self, execution: OrderExecution):
        self.filled_avg_price = (self.quantity * self.filled_avg_price -
                                 execution.filled_quantity * execution.filled_avg_price) / \
                                (self.quantity - execution.filled_quantity)
        self.filled_quantity = self.filled_quantity - execution.filled_quantity
        # if self.quantity == 0:
        #     self.filled_start_time = None
        #     self.filled_end_time = None
        #
        # if self.filled_quantity < self.quantity:
        #     self.status = OrderStatus.CREATED
        self.fee = self.fee - execution.commission
        self.execution_map.pop(execution.id)

    # def order_filled(self, filled_quantity, filled_price, filled_start_time, filled_end_time):
    #     if self.status == OrderStatus.FILLED or self.status == OrderStatus.CANCELED:
    #         raise RuntimeError("wrong order status")
    #     self.filled_quantity += filled_quantity
    #     if self.filled_quantity != self.quantity:
    #         raise RuntimeError(" wrong filled quantity")
    #     self.filled_start_time = filled_start_time
    #     self.filled_end_time = filled_end_time
    #     self.filled_avg_price = filled_price
    #     self.status = OrderStatus.FILLED
    #     logging.info("订单成交:{}".format(self.__dict__))
    #
    # def order_filled_realtime(self, this_filled: float, this_filled_avg_price: float):
    #     self.filled_avg_price = (this_filled * this_filled_avg_price + self.filled_quantity * self.filled_avg_price) \
    #                             / (this_filled + self.filled_quantity)
    #     self.filled_quantity += this_filled
    #     if self.filled_quantity > self.quantity:
    #         raise RuntimeError("wrong filled quantity")
    #     if self.filled_quantity == self.quantity:
    #         self.status = OrderStatus.FILLED
    #         self.filled_end_time = Timestamp.now(tz='Asia/Shanghai')
    #
    #     if not self.filled_start_time:
    #         self.filled_start_time = Timestamp.now(tz='Asia/shanghai')

    def cancel(self):
        self.status = OrderStatus.CANCELED

    def remaining(self):
        return self.quantity - self.filled_quantity

    def __lt__(self, other: Order):
        # 根据filled_start_time进行排序，没有的话，则根据place_time进行排序
        if self.filled_start_time and other.filled_start_time:
            return self.filled_start_time < other.filled_start_time
        else:
            return self.place_time < other.place_time

    def cash_change(self):
        cash_change = 0
        cash_change -= self.fee
        if self.direction == OrderDirection.BUY:
            cash_change = cash_change - self.filled_quantity * self.filled_avg_price
        else:
            cash_change = cash_change + self.filled_quantity * self.filled_avg_price
        return cash_change


class MKTOrder(Order):
    def bar_match(self, bar: Bar) -> OrderExecution:
        # 以开盘价成交
        return OrderExecution(str(uuid.uuid1()), 0, 0, self.quantity, bar.open_price, bar.start_time,
                              bar.visible_time, self.direction)

    def tick_match(self, tick: Tick) -> OrderExecution:
        return OrderExecution(str(uuid.uuid1()), 0, 0, self.quantity, tick.price,
                              tick.visible_time, tick.visible_time, self.direction)

    def __init__(self, code, direction, quantity, place_time, quantity_split: List[int] = None):
        super().__init__(code, direction, quantity, place_time, quantity_split)


class DelayMKTOrder(Order):

    def __init__(self, code, direction, quantity, place_time, delay_time: Timedelta, quantity_split: List[int] = None):
        super().__init__(code, direction, quantity, place_time, quantity_split)
        self.delay_time = delay_time

    def bar_match(self, bar: Bar):
        if bar.start_time >= (self.place_time + self.delay_time):
            return OrderExecution(str(uuid.uuid1()), 0, 0, self.quantity, bar.open_price, bar.start_time,
                                  bar.visible_time, self.direction)
        elif bar.visible_time >= (self.place_time + self.delay_time):
            return OrderExecution(str(uuid.uuid1()), 0, 0, self.quantity, bar.close_price, bar.start_time,
                                  bar.visible_time, self.direction)
        return None

    def tick_match(self, tick: Tick):
        if tick.visible_time >= (self.place_time + self.delay_time):
            return OrderExecution(str(uuid.uuid1()), 0, 0, self.quantity, tick.price,
                                  tick.visible_time, tick.visible_time, self.direction)
        return None


class LimitOrder(Order):

    def __init__(self, code, direction, quantity, place_time, limit_price, quantity_split: List[int] = None):
        super().__init__(code, direction, quantity, place_time, quantity_split)
        self.limit_price = limit_price

    def bar_match(self, bar: Bar):
        if self.direction == OrderDirection.BUY:
            if bar.low_price <= self.limit_price:
                return OrderExecution(str(uuid.uuid1()), 0, 0, self.quantity, self.limit_price, bar.start_time,
                                      bar.visible_time, self.direction)
        else:
            if bar.high_price >= self.limit_price:
                return OrderExecution(str(uuid.uuid1()), 0, 0, self.quantity, self.limit_price, bar.start_time,
                                      bar.visible_time, self.direction)
        return None

    def tick_match(self, tick: Tick):
        if self.direction == OrderDirection.BUY:
            if tick.price <= self.limit_price:
                return OrderExecution(str(uuid.uuid1()), 0, 0, self.quantity, self.limit_price,
                                      tick.visible_time, tick.visible_time, self.direction)
        else:
            if tick.price >= self.limit_price:
                return OrderExecution(str(uuid.uuid1()), 0, 0, self.quantity, self.limit_price,
                                      tick.visible_time, tick.visible_time, self.direction)
        return None


class CrossMKTOrder(Order):

    def __init__(self, code, direction, quantity, place_time, cross_direction: CrossDirection, cross_price,
                 quantity_split: List[int] = None):
        super().__init__(code, direction, quantity, place_time, quantity_split)
        self.cross_direction = cross_direction
        self.cross_price = cross_price

    def bar_match(self, bar: Bar):
        if self.cross_direction == CrossDirection.UP:
            if bar.high_price >= self.cross_price:
                return OrderExecution(str(uuid.uuid1()), 0, 0, self.quantity, self.cross_price, bar.start_time,
                                      bar.visible_time, self.direction)

        else:
            if bar.low_price <= self.cross_price:
                return OrderExecution(str(uuid.uuid1()), 0, 0, self.quantity, self.cross_price, bar.start_time,
                                      bar.visible_time, self.direction)

        return None

    def tick_match(self, tick: Tick):
        if self.cross_direction == CrossDirection.UP:
            if tick.price >= self.cross_price:
                return OrderExecution(str(uuid.uuid1()), 0, 0, self.quantity, self.cross_price, tick.visible_time,
                                      tick.visible_time, self.direction)
        else:
            if tick.price <= self.cross_price:
                return OrderExecution(str(uuid.uuid1()), 0, 0, self.quantity, self.cross_price, tick.visible_time,
                                      tick.visible_time, self.direction)
        return None


class Operation(object):
    def __init__(self, start_cash: float):
        self.start_time = None
        self.end_time = None
        self.pnl = 0
        self.orders: List[Order] = []
        self.start_cash = start_cash

    def end(self, start_cash: float):
        if len(self.orders) <= 0:
            raise RuntimeError("操作没有订单")
        self.end_time = self.orders[-1].filled_end_time
        self.pnl = self.calc_pnl()
        return Operation(start_cash)

    def add_order(self, order: Order):
        if len(self.orders) <= 0:
            self.start_time = order.place_time
        self.orders.append(order)

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

    def __init__(self, name: str, initial_cash: float):
        self.name = name
        self.cash = initial_cash
        self.initial_cash = initial_cash
        self.positions = {}
        self.history_net_value: Mapping[Timestamp, float] = {}
        self.orders: List[Order] = []
        self.order_callback = None
        # self.current_operation: Operation = Operation(initial_cash)
        # self.history_operations: List[Operation] = []

    def with_order_callback(self, order_callback: OrderCallback):
        self.order_callback = order_callback
        return self

    @abstractmethod
    def place_order(self, order: Order):
        pass

    def get_open_orders(self):
        ods = []
        for order in self.orders:
            if order.status == OrderStatus.CREATED or order.status == OrderStatus.PARTIAL_FILLED or \
                    order.status == OrderStatus.SUBMITTED:
                ods.append(order)
        return ods

    @abstractmethod
    def match(self, data):
        pass

    @abstractmethod
    def valid_scope(self, data):
        pass

    def save(self):
        account_repo: AccountRepo = BeanContainer.getBean(AccountRepo)
        account_repo.save(self)

    def calc_net_value(self, current_price: Mapping[str, float], current_time: Timestamp):
        net_value = 0
        net_value += self.cash
        for code in self.positions.keys():
            net_value += current_price[code] * self.positions[code]
        self.history_net_value[current_time] = net_value

    def cancel_all_open_orders(self):
        for open_order in self.get_open_orders():
            open_order.cancel()
            self.order_callback.order_status_change(open_order, self)

    @do_log(target_name='订单成交', escape_params=[EscapeParam(index=0, key='self')])
    @alarm(level=AlarmLevel.NORMAL, target='订单成交', escape_params=[EscapeParam(index=0, key='self')])
    def order_filled(self, order: Order, execution: OrderExecution):
        if execution.id in order.execution_map:
            old_execution = order.execution_map[execution.id]
            if execution.version > old_execution.version:
                self._reverse(order, old_execution)
                self._order_filled(order, execution)
        else:
            self._order_filled(order, execution)

    def _order_filled(self, order: Order, execution: OrderExecution):
        # 修改账户现金
        self.cash += execution.cash_change()

        # 修改持仓
        position_change = execution.filled_quantity
        if order.direction == OrderDirection.SELL:
            position_change = -position_change
        self.update_position(order.code, position_change)

        # 修改订单的状态
        order.order_filled(execution)
        if self.order_callback:
            self.order_callback.order_status_change(order, self)

    def _reverse(self, order: Order, execution: OrderExecution):
        self.cash -= execution.cash_change()

        # 修改持仓
        position_change = execution.filled_quantity
        if order.direction == OrderDirection.BUY:
            position_change = -position_change
        self.update_position(order.code, position_change)
        # 修改订单状态
        order.reverse(execution)
        self.order_callback.order_status_change(order, self)

    def update_position(self, code, position_change):
        if code in self.positions:
            self.positions[code] = self.positions[code] + position_change
        else:
            self.positions[code] = position_change

        if self.positions[code] == 0:
            self.positions.pop(code)

    def history_operations(self) -> List[Operation]:
        filled_orders = [o for o in self.orders if o.filled_quantity > 0]
        filled_orders = sorted(filled_orders)
        mocked_positions = {}
        mocked_cash = self.initial_cash
        operations: List[Operation] = []
        current_operation = Operation(mocked_cash)

        def position_change(positions: Dict, order: Order):
            pc = order.filled_quantity
            if order.direction == OrderDirection.SELL:
                pc = -pc
            if order.code in positions:
                positions[order.code] = positions[order.code] + pc
            else:
                positions[order.code] = pc
            if positions[order.code] == 0:
                positions.pop(order.code)

        for o in filled_orders:
            current_operation.add_order(o)
            mocked_cash += o.cash_change()
            position_change(mocked_positions, o)
            if len(mocked_positions) <= 0:
                new_operation = current_operation.end(mocked_cash)
                operations.append(current_operation)
                current_operation = new_operation

        operations.append(current_operation)
        return operations

    def net_value(self, current_prices: Dict[str, float]):
        net_value = 0
        net_value += self.cash
        for code in self.positions.keys():
            if code not in current_prices:
                raise RuntimeError("缺少价格数据")
            net_value += self.positions[code] * current_prices[code]
        return net_value

    @abstractmethod
    def cancel_open_order(self, open_order):
        pass

    @abstractmethod
    def update_order(self, order, reason):
        pass


class AccountRepo(metaclass=ABCMeta):
    @abstractmethod
    def save(self, self1):
        pass

    @abstractmethod
    def find_one(self, account_name):
        pass


class BacktestAccount(AbstractAccount):

    def cancel_open_order(self, open_order):
        raise NotImplementedError

    def update_order(self, order, reason):
        raise NotImplementedError

    def valid_scope(self, data):
        pass

    def match(self, data):
        open_orders = self.get_open_orders()
        for o in open_orders:
            if o.status != OrderStatus.CREATED:
                continue
            execution: OrderExecution = None
            if isinstance(data, Bar):
                execution = o.bar_match(data)
            elif isinstance(data, Tick):
                execution = o.tick_match(data)
            else:
                raise RuntimeError("wrong data")
            if execution:
                # change order status
                self.order_filled(o, execution)

                # # 修改账户现金
                # if o.direction == OrderDirection.SELL:
                #     self.cash += execution.filled_quantity * execution.filled_price
                # else:
                #     self.cash -= execution.filled_quantity * execution.filled_price
                #
                # # 修改持仓
                # quantity_change = execution.filled_quantity
                # if o.direction == OrderDirection.SELL:
                #     quantity_change = -quantity_change
                # if o.code in self.positions:
                #     self.positions[o.code] += quantity_change
                # else:
                #     self.positions[o.code] = quantity_change
                #
                # if self.positions[o.code] == 0:
                #     self.positions.pop(o.code)

                # if len(self.positions) <= 0:
                #     next_operation = self.current_operation.end(self.cash)
                #     self.history_operations.append(self.current_operation)
                #     self.current_operation = next_operation

                # self.order_callback.order_status_change(o, self)

    def place_order(self, order: Order):
        logging.info("下单：{}".format(str(order.__dict__)))
        self.orders.append(order)
