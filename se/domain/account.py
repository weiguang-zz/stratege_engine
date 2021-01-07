import logging
from abc import *
from enum import Enum
from typing import *

from ibapi.common import OrderId
from ibapi.contract import Contract, ContractDetails
from ibapi.order_condition import OrderCondition, PriceCondition
from ibapi.order_state import OrderState
from pandas import Timedelta
from pandas import Timestamp

from se.domain.data_portal import Bar
from se.domain.event_producer import EventProducer, Event, EventType, EventSubscriber
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.order import Order as IBOrder
from ibapi import order_condition
from threading import Condition
import random


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
        return "order:[{}], price:{}, quantity:{}, start_filled_time:{}, end_filled_time:{}". \
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
        self.commission = 0

    def __str__(self):
        return "code:{}, order_type:{}, direction:{}, quantity:{}, place_time:{}, delay_time:{}, " \
               "start_filled_time:{}, end_filled_time:{}, filled:{}, limit_price:{}, status:{}, " \
               "cross_price:{}, cross_direction:{}, commission:{}".format(self.code, self.order_type, self.direction,
                                                                          self.quantity, self.place_time,
                                                                          self.delay_time,
                                                                          self.start_filled_time, self.end_filled_time,
                                                                          self.filled,
                                                                          self.limit_price, self.status,
                                                                          self.cross_price,
                                                                          self.cross_price, self.commission)


class AbstractAccount(EventProducer, metaclass=ABCMeta):

    def __init__(self, initial_cash: float):
        super().__init__()
        self.initial_cash = initial_cash
        self.cash = initial_cash
        self.daily_net_values = {}

    @abstractmethod
    def place_order(self, order):
        pass

    @abstractmethod
    def get_positions(self) -> Dict[str, Position]:
        pass

    @abstractmethod
    def get_open_orders(self):
        pass

    def calc_daily_net_value(self, time: Timestamp, recent_prices: Dict[str, float]):
        positions = self.get_positions()
        daily_net_value = self.cash
        for code in positions.keys():
            if code not in recent_prices:
                raise RuntimeError("没有资产的最新价格，无法计算每日净值")
            daily_net_value += positions[code].quantity * recent_prices[code]

        self.daily_net_values[time] = daily_net_value

    @abstractmethod
    def cancel_all_open_orders(self):
        pass


class BacktestAccount(AbstractAccount):

    def start_listen(self, subscriber):
        raise RuntimeError("不支持订阅回测账户")

    def get_events_on_history(self, visible_time_start: Timestamp, visible_time_end: Timestamp):
        raise RuntimeError("不支持的操作")

    def __init__(self, initial_cash: float):
        super(BacktestAccount, self).__init__(initial_cash)
        self.positions: Dict[str, Position] = {}
        self.orders = []

    def get_positions(self):
        return self.positions

    def get_open_orders(self):
        open_orders = []
        for order in self.orders:
            if order.status == OrderStatus.CREATED:
                open_orders.append(order)
        return open_orders

    def place_order(self, order: Order):
        logging.info("下单:" + str(order))
        self.orders.append(order)

    def order_filled(self, order_filled_data: OrderFilledData):
        """
        :return:
        """
        logging.info("订单成交:" + str(order_filled_data))
        if order_filled_data.order.status == OrderStatus.FILLED or \
                order_filled_data.order.status == OrderStatus.CANCELED:
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

    def cancel_all_open_orders(self):
        for order in self.get_open_orders():
            order.status = OrderStatus.CANCELED


class Request(object):
    id_to_request = {}

    def __init__(self):
        self.condition: Condition = Condition()
        self.req_id = self.random_id()
        self.resp = None
        Request.id_to_request[self.req_id] = self

    def random_id(self):
        while True:
            k = random.randint(0, 100000000)
            if k not in Request.id_to_request:
                return k


class IBAccount(AbstractAccount, EWrapper):
    def orderStatus(self, orderId: OrderId, status: str, filled: float, remaining: float, avgFillPrice: float,
                    permId: int, parentId: int, lastFillPrice: float, clientId: int, whyHeld: str, mktCapPrice: float):
        super().orderStatus(orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId,
                            whyHeld, mktCapPrice)
        if orderId not in self.ib_order_id_to_order:
            raise RuntimeError("orderId非法")
        # 由于目前策略只支持filled和cancelled两种状态，所以先只考虑这两种状态
        order: Order = self.ib_order_id_to_order[orderId]
        now = Timestamp.now(tz='Asia/Shanghai')
        if status == 'Filled':
            # 订单成交
            if order.status == OrderStatus.FILLED:
                #  表示已经发送过成交事件
                logging.info("重复的订单成交消息，不做处理")
                return
            data: OrderFilledData = OrderFilledData(order, avgFillPrice, filled, start_filled_time=now,
                                                    end_filled_time=now)
            self.order_filled(data)
            event = Event(event_type=EventType.ACCOUNT, sub_type="order_filled",
                          visible_time=Timestamp.now(tz="Asia/Shanghai"), data=data)
            self.subscriber.on_event(event)
        if status == "Cancelled":
            event = Event(event_type=EventType.ACCOUNT, sub_type="order_cancelled", visible_time=now, data=None)
            self.subscriber.on_event(event)

    def updateAccountValue(self, key: str, val: str, currency: str, accountName: str):
        super().updateAccountValue(key, val, currency, accountName)

    def updatePortfolio(self, contract: Contract, position: float, marketPrice: float, marketValue: float,
                        averageCost: float, unrealizedPNL: float, realizedPNL: float, accountName: str):
        super().updatePortfolio(contract, position, marketPrice, marketValue, averageCost, unrealizedPNL, realizedPNL,
                                accountName)
        code = self.contract_to_code(contract)
        self.positions[code] = Position(code, position)

    def openOrder(self, orderId: OrderId, contract: Contract, order: Order, orderState: OrderState):
        super().openOrder(orderId, contract, order, orderState)
        # 设置佣金
        if orderId not in self.ib_order_id_to_order:
            raise RuntimeError("orderId非法")
        self.ib_order_id_to_order[orderId].commission = orderState.commission

    def openOrderEnd(self):
        super().openOrderEnd()

    def contractDetails(self, reqId: int, contractDetails: ContractDetails):
        super().contractDetails(reqId, contractDetails)
        if reqId not in Request.id_to_request:
            raise RuntimeError("非法的请求id：" + reqId)
        req: Request = Request.id_to_request[reqId]
        req.resp = contractDetails
        if req.condition.acquire():
            req.condition.notifyAll()
            req.condition.release()

    def get_positions(self) -> Dict[str, Position]:
        return self.positions

    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self._next_valid_id = orderId

    def next_id(self):
        self._next_valid_id += 1
        return self._next_valid_id

    def get_open_orders(self):
        results = []
        for od in self.orders:
            if od.status == OrderStatus.CREATED:
                results.append(od)
        return results

    def cancel_all_open_orders(self):
        for ib_order_id in self.ib_order_id_to_order.keys():
            if self.ib_order_id_to_order[ib_order_id].status == OrderStatus.CREATED:
                self.client.cancelOrder(ib_order_id)

    def place_order(self, order):
        self.orders.append(order)
        ib_order_id = self.next_id()
        order.ib_order_id = ib_order_id
        self.ib_order_id_to_order[ib_order_id] = order
        ib_order = self.get_ib_order(order)
        logging.info("下单：" + str(ib_order))
        self.client.placeOrder(ib_order_id, self.get_contract(order.code), ib_order)

    def start_listen(self, subscriber: EventSubscriber):
        self.subscriber = subscriber

    def get_events_on_history(self, visible_time_start: Timestamp, visible_time_end: Timestamp):
        raise NotImplementedError

    def __init__(self, host, port, client_id, initial_cash: float):
        super().__init__(initial_cash)
        client = EClient(self)
        self.client = client
        self.positions = {}
        self.orders: List[Order] = []
        self._next_valid_id = None
        self.ib_order_id_to_order: Dict[int, Order] = {}
        self.contract_code_to_detail = {}
        client.connect(host, port, client_id)
        import threading
        threading.Thread(name="ib_msg_consumer", target=client.run).start()

    def get_contract(self, code: str) -> Contract:
        contract = Contract()
        ss = code.split("_")
        contract.symbol = ss[0]
        contract.secType = ss[1]
        contract.currency = ss[2]
        contract.exchange = ss[3]
        contract_detail: ContractDetails = self.sync_get_contract_detail(contract)
        contract.conId = contract_detail.contract.conId
        return contract

    def get_ib_order(self, order: Order) -> IBOrder:
        # 市价单和限价单直接提交
        ib_order: IBOrder = IBOrder()
        if order.order_type == OrderType.MKT:
            ib_order.orderType = "MKT"
            ib_order.totalQuantity = order.quantity
            ib_order.action = 'BUY' if order.direction == OrderDirection.BUY else 'SELL'
        elif order.order_type == OrderType.LIMIT:
            ib_order.orderType = "LMT"
            ib_order.totalQuantity = order.quantity
            ib_order.lmtPrice = order.limit_price
            ib_order.action = 'BUY' if order.direction == OrderDirection.BUY else 'SELL'
        else:
            # 穿越单和延迟单转化为IB的条件单
            if order.order_type == OrderType.DELAY_MKT:
                ib_order.orderType = "MKT"
                ib_order.totalQuantity = order.quantity
                ib_order.action = 'BUY' if order.direction == OrderDirection.BUY else 'SELL'
                cond = order_condition.Create(OrderCondition.Time)
                cond.isMore = True
                time = (Timestamp.now(tz='Asia/Shanghai') + order.delay_time).strftime('%Y%m%d %H:%M:%S')
                cond.time(time)
                ib_order.conditions.append(cond)

            elif order.order_type == OrderType.CROSS_MKT:
                ib_order.orderType = "MKT"
                ib_order.totalQuantity = order.quantity
                ib_order.action = 'BUY' if order.direction == OrderDirection.BUY else 'SELL'
                price_cond = order_condition.Create(OrderCondition.Price)
                contract = self.get_contract(order.code)
                price_cond.conId = contract.conId
                price_cond.price = order.cross_price
                price_cond.isMore = True if order.cross_direction == CrossDirection.UP else False
                price_cond.exchange = contract.exchange
                price_cond.triggerMethod = PriceCondition.TriggerMethodEnum.Default
                ib_order.conditions.append(price_cond)

        return ib_order

    @classmethod
    def contract_to_code(cls, contract):
        return '{}_{}_{}_{}'.format(contract.symbol, contract.secType, contract.currency, contract.exchange)

    def sync_get_contract_detail(self, contract: Contract) -> ContractDetails:
        code = self.contract_to_code(contract)
        if code in self.contract_code_to_detail:
            return self.contract_code_to_detail[code]
        req = Request()
        self.client.reqContractDetails(req.req_id, contract)
        # 等待5s
        if req.condition.acquire():
            req.condition.wait(5)
        if not req.resp:
            raise RuntimeError("没有获取到合约数据")
        resp = req.resp
        # 清理数据
        Request.id_to_request.pop(req.req_id)
        self.contract_code_to_detail[code] = resp
        return resp

    def order_filled(self, data: OrderFilledData):
        order = data.order
        if order.status == OrderStatus.FILLED or order.status == OrderStatus.CANCELED:
            raise RuntimeError("非法的订单状态")
        order.filled = data.quantity
        order.status = OrderStatus.FILLED
