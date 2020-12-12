from abc import *
from enum import Enum
from typing import Dict

from pandas import Timedelta
from pandas import Timestamp

from main.domain.engine import StrategyEngine
from main.domain.event_producer import Event, EventType, AccountEventType, EventProducer


class OrderType(Enum):
    MKT = 0
    LIMIT = 1


class OrderDirection(Enum):
    BUY = 0
    SELL = 1


class OrderStatus(Enum):
    CREATED = 0
    CANCELED = 1
    FILLED = 2


class Position(object):

    def __init__(self, code: str, quantity: float):
        self.code = code
        self.quantity = quantity


class Order(object):

    def __init__(self, code: str, quantity: float, order_type: OrderType, direction: OrderDirection, time: Timestamp,
                 limit_price: float):
        self.code = code
        self.quantity = quantity
        self.order_type = order_type
        self.direction = direction
        self.place_time = time
        self.filled_time = None
        self.filled = 0
        self.limit_price = limit_price
        self.status = OrderStatus.CREATED


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

    def start_listen(self, subscriber: StrategyEngine):
        raise RuntimeError("不支持订阅回测账户")

    def get_events_on_history(self, visible_time_start: Timestamp, visible_time_end: Timestamp):
        raise RuntimeError("不支持的操作")

    def __init__(self, initial_cash: float):
        super(BacktestAccount, self).__init__(initial_cash)

    def place_order(self, order: Order):
        self.orders.append(order)

    def order_filled(self, order: Order, quantity: float, price: float, start_filled_time: Timestamp):
        """
        :param order: 订单
        :param quantity: 成交数量
        :param open_price: 成交价格
        :param visible_time: 订单成交可见时间
        :return:
        """
        if order.status == OrderStatus.FILLED or order.status == OrderStatus.CANCELED:
            raise RuntimeError("订单状态非法")

        order.filled += quantity
        if order.filled != order.quantity:
            raise RuntimeError("订单成交数据错误")
        order.status = OrderStatus.FILLED
        order.filled_time = start_filled_time
        # 扣减现金，增加持仓
        if order.direction == OrderDirection.SELL:
            quantity = -quantity
        self.cash -= price * quantity
        if order.code not in self.positions:
            self.positions[order.code] = Position(order.code, quantity)
        else:
            self.positions[order.code].quantity += quantity