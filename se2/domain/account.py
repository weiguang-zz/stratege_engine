from __future__ import annotations

import uuid

from se2.domain.common import *
from se2.domain.time_series import *


class OrderStatusCallback(metaclass=ABCMeta):
    @abstractmethod
    def order_status_change(self, order):
        pass


class OrderDirection(Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(Enum):
    CREATED = "CREATED"
    SUBMITTED = "SUBMITTED"
    FAILED = "FAILED"
    PARTIAL_FILLED = "PARTIAL_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"


class ExecutionFrom(object):
    """
    表示一个执行详情来自于哪里，可以是回测引擎、盈透或者德美利等
    """

    def __init__(self):
        pass


class Execution(object):

    def __init__(self, id: str, version: int, quantity: float, price: float, time: Timestamp, fee: float,
                 real_order_id: str):
        self.id = id
        self.version = version
        self.quantity = quantity
        self.price = price
        self.time = time
        self.fee = fee
        self.real_order_id = real_order_id


class Order(metaclass=ABCMeta):

    def __init__(self, code, direction, quantity, place_time, reason, ideal_price):
        self.code = code
        self.direction = direction
        self.quantity = quantity
        self.place_time = place_time
        self.status = OrderStatus.CREATED
        self.executions: Dict[str, Execution] = {}
        self.account_name = None
        self.reason = reason
        self.remark = None
        self.real_order_ids: List = []
        self.ideal_price = ideal_price
        # 成交数据
        self.filled_start_time = None
        self.filled_end_time = None
        self.filled_quantity = 0
        self.filled_avg_price = 0
        self.fee = 0
        # 成交数据
        self.order_status_callback: OrderStatusCallback = None
        self.failed_reason = None
        self.cancel_reason = None
        # 是否允许延长时段交易
        self.extended_time = False

    def with_order_status_callback(self, order_status_callback: OrderStatusCallback):
        self.order_status_callback = order_status_callback
        return self

    def submitted(self):
        if self.status in [OrderStatus.CANCELED, OrderStatus.FAILED, OrderStatus.SUBMITTED]:
            raise RuntimeError("非法的订单状态")
        elif self.status in [OrderStatus.FILLED, OrderStatus.PARTIAL_FILLED]:
            # do nothing
            pass
        elif self.status == OrderStatus.CREATED:
            self.status = OrderStatus.SUBMITTED
            if self.order_status_callback:
                self.order_status_callback.order_status_change(self)

    @do_log(target_name="订单失败")
    def failed(self, reason, real_order_id: str = None):
        if self.status == OrderStatus.FAILED:
            return
        if real_order_id and real_order_id not in self.real_order_ids:
            raise RuntimeError('非法的real_order_id:{}'.format(real_order_id))
        if self.status in [OrderStatus.CREATED, OrderStatus.SUBMITTED]:
            if real_order_id and len(self.real_order_ids) > 1:
                self.real_order_ids.remove(real_order_id)
            else:
                self.failed_reason = reason
                self.status = OrderStatus.FAILED
                if self.order_status_callback:
                    self.order_status_callback.order_status_change(self)
                self.save()
        else:
            raise RuntimeError("非法的订单状态")

    @do_log(target_name="订单取消")
    def cancelled(self, reason, real_order_id: str = None):
        if real_order_id and real_order_id not in self.real_order_ids:
            raise RuntimeError('非法的real_order_id:{}'.format(real_order_id))
        if self.status in [OrderStatus.SUBMITTED, OrderStatus.PARTIAL_FILLED]:
            if real_order_id and len(self.real_order_ids) > 1:
                self.real_order_ids.remove(real_order_id)
            else:
                self.cancel_reason = reason
                self.status = OrderStatus.CANCELED
                if self.order_status_callback:
                    self.order_status_callback.order_status_change(self)
                self.save()
        else:
            raise RuntimeError("非法的订单状态")

    @abstractmethod
    def bar_match(self, bar: Bar) -> Execution:
        pass

    @abstractmethod
    def current_price_match(self, current_price: CurrentPrice) -> Execution:
        pass

    def order_filled(self, execution: Execution):
        if not execution.real_order_id.startswith("bt") and execution.real_order_id not in self.real_order_ids:
            raise RuntimeError('非法的执行详情:{}'.format(execution.__dict__))
        if execution.id in self.executions:
            old_exec = self.executions[execution.id]
            if execution.version > old_exec.version:
                self.executions[execution.id] = execution
        else:
            self.executions[execution.id] = execution

        self._re_compute_filled_data()

    def replace_order_filled(self, new_executions: List[Execution]):
        self.executions = {}
        for execution in new_executions:
            self.executions[execution.id] = execution

        self._re_compute_filled_data()

    def _re_compute_filled_data(self):
        total_filled_quantity = 0
        total_net_value = 0
        filled_start_time = None
        filled_end_time = None
        total_fee = 0

        for execution in self.executions.values():
            total_fee += execution.fee
            total_net_value = execution.price * execution.quantity
            total_filled_quantity += execution.quantity
            if not filled_start_time or execution.time < filled_start_time:
                filled_start_time = execution.time
            if not filled_end_time or execution.time > filled_end_time:
                filled_end_time = execution.time

        self.filled_quantity = total_filled_quantity
        self.filled_avg_price = total_net_value / total_filled_quantity
        self.fee = total_fee
        self.filled_start_time = filled_start_time
        self.filled_end_time = filled_end_time

        if self.filled_quantity > self.quantity:
            raise RuntimeError("非法的成交数量")
        elif self.filled_quantity == self.quantity:
            if self.status in [OrderStatus.CREATED, OrderStatus.SUBMITTED, OrderStatus.PARTIAL_FILLED]:
                self.status = OrderStatus.FILLED
                self.save()
            else:
                raise RuntimeError("非法的订单状态")
        elif self.filled_quantity > 0:
            if self.status in [OrderStatus.CREATED, OrderStatus.SUBMITTED]:
                self.status = OrderStatus.PARTIAL_FILLED
            else:
                raise RuntimeError("非法的订单状态")

    def cash_cost(self):
        """
        该笔订单消耗的现金，如果是负的，表示这笔订单增加了现金
        :return:
        """
        cost = self.fee
        if self.direction == OrderDirection.BUY:

            cost += self.filled_quantity * self.filled_avg_price
        else:
            cost = cost - self.filled_quantity * self.filled_avg_price
        return cost

    def with_status_callback(self, order_status_callback):
        self.order_status_callback = order_status_callback
        return self

    def with_account(self, account_name):
        self.account_name = account_name
        return self

    def append_read_order_id(self, real_order_id):
        self.real_order_ids.append(real_order_id)

    def save(self):
        order_repo: OrderRepo = BeanContainer.getBean(OrderRepo)
        order_repo.save(self)

    def __str__(self):
        return "direction:{}, code:{}, quantity:{}, status:{}, place_time:{}, filled_end_time:{}". \
            format(self.direction, self.code, self.quantity, self.status, self.place_time, self.filled_end_time)


class PriceChange(object):
    def __init__(self, time: Timestamp, pre_price: float, after_price: float, current_price: CurrentPrice):
        self.time = time
        self.pre_price = pre_price
        self.after_price = after_price
        self.current_price = current_price


class LimitOrder(Order):

    def __init__(self, code, direction, quantity, place_time, reason, ideal_price, limit_price: float,
                 bargainer: Bargainer = None):
        super().__init__(code, direction, quantity, place_time, reason, ideal_price)
        self.limit_price = limit_price
        self.bargainer: Bargainer = bargainer
        if self.limit_price and bargainer:
            raise RuntimeError("不能同时制定limit_price和bargainer")
        if not self.limit_price and not bargainer:
            raise RuntimeError("limit price和bargainer必须指定一个")
        if self.bargainer:
            self.bargainer.bind_order(self)

    def bar_match(self, bar: Bar) -> Execution:
        if self.direction == OrderDirection.BUY:
            if bar.low <= self.limit_price:
                # 如果bar完全在limit_price下面的话，则成交价定位high和limit_price的较低者
                return Execution(str(uuid.uuid1()), 0, self.quantity, min(bar.high, self.limit_price),
                                 bar.visible_time, 0, 'bt')
        else:
            if bar.high >= self.limit_price:
                # 如果bar完全在limit_price上面的话，则成交价定位low和limit_price的较高者
                return Execution(str(uuid.uuid1()), 0, self.quantity, max(self.limit_price, bar.low),
                                 bar.visible_time, 0, 'bt')
        return None

    def current_price_match(self, current_price: CurrentPrice) -> Execution:
        if self.direction == OrderDirection.BUY:
            if current_price.price <= self.limit_price:
                return Execution(str(uuid.uuid1()), 0, self.quantity, current_price.price,
                                 current_price.visible_time, 0, 'bt')
        else:
            if current_price.price >= self.limit_price:
                return Execution(str(uuid.uuid1()), 0, self.quantity, current_price.price,
                                 current_price.visible_time, 0, 'bt')
        return None

    def __str__(self):
        return "limit_price:{},{}".format(self.limit_price, super(LimitOrder, self).__str__())


class MKTOrder(Order):
    def bar_match(self, bar: Bar) -> Execution:
        # 以开盘价成交
        return Execution(str(uuid.uuid1()), 0, self.quantity, bar.open, bar.visible_time,
                         0, "bt")

    def current_price_match(self, current_price: CurrentPrice) -> Execution:
        return Execution(str(uuid.uuid1()), 0, self.quantity, current_price.price, current_price.visible_time,
                         0, "bt")

    def __init__(self, code, direction, quantity, place_time, reason, ideal_price):
        super().__init__(code, direction, quantity, place_time, reason, ideal_price)


class StopOrder(Order):

    def bar_match(self, bar: Bar) -> Execution:
        if self.direction == OrderDirection.BUY:
            if bar.high >= self.stop_price:
                # 如果bar完全位于stop_price上方时，成交价设置为bar.low
                return Execution(str(uuid.uuid1()), 0, self.quantity, max(bar.low, self.stop_price), bar.visible_time,
                                 0, "bt")
        else:
            if bar.low <= self.stop_price:
                # 如果bar完全位于stop_price的下方时，成交价设置为bar.high
                return Execution(str(uuid.uuid1()), 0, self.quantity, min(bar.high, self.stop_price), bar.visible_time,
                                 0, "bt")
        return None

    def current_price_match(self, current_price: CurrentPrice) -> Execution:
        pass

    def __init__(self, code, direction, quantity, place_time, reason, ideal_price, stop_price: float):
        super().__init__(code, direction, quantity, place_time, reason, ideal_price)
        self.stop_price = stop_price


class Bargainer(object):

    def __init__(self, account: AbstractAccount, current_price_ts: TimeSeries, freq: int,
                 algo: BargainAlgo, time_out_threshold: Timestamp = None,
                 max_deviation_percentage: float = 0.01):
        """
        :param account: 账户
        :param current_price_ts: 实时价格的时间序列
        :param freq: 议价频率
        :param algo 议价算法
        """
        self.account: AbstractAccount = account
        self.current_price_ts: TimeSeries = current_price_ts
        self.freq: int = freq
        self.algo: BargainAlgo = algo
        self.order: LimitOrder = None
        self.current_price_history: List[CurrentPrice] = []
        self.price_change_history: List[PriceChange] = []
        self.time_out_threshold = time_out_threshold
        self.max_deviation_percentage = max_deviation_percentage
        if time_out_threshold:
            if time_out_threshold.tzname() != 'CST':
                raise RuntimeError("时区需要是CST")

    def bind_order(self, order: LimitOrder):
        self.order = order

    def start_bargin(self):
        def do_start():
            # 由于下完单之后立马启动的bargain线程，所以等待一段时间再进行议价流程
            time.sleep(self.freq)
            while True:
                if self.order.status in [OrderStatus.SUBMITTED, OrderStatus.PARTIAL_FILLED]:
                    try:
                        cp: CurrentPrice = self.current_price_ts.current_price([self.order.code])[self.order.code]
                        self.current_price_history.append(cp)
                        if self.time_out_threshold and Timestamp.now(tz='Asia/Shanghai') > self.time_out_threshold:
                            self.algo.timeout(cp, self)
                            logging.info("议价超时结束")
                            break
                        price_change: PriceChange = self.algo.bargain(cp, self)
                        if price_change:
                            # 由于订单执行详情是在异步的线程中更新的，所以这个时候订单可能已经成交的
                            # 在任何跟订单状态有关系的操作之前，都进行这个判断是合理的
                            if self.order.status in [OrderStatus.SUBMITTED, OrderStatus.PARTIAL_FILLED]:
                                price_change.after_price = self.revise_price_if_needed(price_change.after_price)
                                self.account.update_order_price(self.order, price_change.after_price)
                                self.price_change_history.append(price_change)
                                self.latest_price = price_change.after_price

                    except:
                        import traceback
                        logging.error("议价异常{}".format(traceback.format_exc()))
                    time.sleep(self.freq)
                else:
                    logging.info("议价结束")
                    break

        threading.Thread(target=do_start, name='bargin thread').start()

    def get_initial_price(self):
        cp = self.current_price_ts.current_price([self.order.code])[self.order.code]
        price: float = self.algo.get_initial_price(cp, self)
        price = self.revise_price_if_needed(price)
        self.price_change_history.append(PriceChange(cp.visible_time, -1, price, cp))
        return price

    def revise_price_if_needed(self, new_price) -> float:
        """
        通过跟订单的理想价格相比较，校验出价是否超出了可接受的偏差。 并且将价格的小数位限制为2位
        """
        if self.order.direction == OrderDirection.BUY:
            price_limit = self.order.ideal_price * (1 + self.max_deviation_percentage)
            if new_price > price_limit:
                logging.info("要设置的价格超过了最大的允许偏差，将设置价格为临界值:" + str(price_limit))
                return round(price_limit, 2)
            else:
                return round(new_price, 2)
        elif self.order.direction == OrderDirection.SELL:
            price_limit = self.order.ideal_price * (1 - self.max_deviation_percentage)
            if new_price < price_limit:
                logging.info("要设置的价格超过了最大的允许偏差，将设置价格为临界值:" + str(price_limit))
                return round(price_limit, 2)
            else:
                return round(new_price, 2)
        else:
            raise RuntimeError("wrong order direction")

    def last_price(self) -> float:
        """
        最新的出价
        :return:
        """
        return self.price_change_history[-1].after_price


class BargainAlgo(metaclass=ABCMeta):

    @abstractmethod
    def get_initial_price(self, cp: CurrentPrice, bargainer: Bargainer) -> float:
        pass

    @abstractmethod
    def bargain(self, cp: CurrentPrice, bargainer: Bargainer) -> PriceChange:
        pass

    def timeout(self, cp: CurrentPrice, bargainer: Bargainer):
        if bargainer.order.status in [OrderStatus.SUBMITTED, OrderStatus.PARTIAL_FILLED]:
            bargainer.account.cancel_order(bargainer.order, "议价超时")


class DefaultBargainAlgo(BargainAlgo):

    def get_initial_price(self, cp: OrderStatus, bargainer: Bargainer) -> float:
        if bargainer.order.direction == OrderDirection.BUY:
            return round(cp.bid_price + self.delta, 2)
        elif bargainer.order.direction == OrderDirection.SELL:
            return round(cp.ask_price - self.delta, 2)
        else:
            raise RuntimeError("wrong direction")
        pass

    def bargain(self, cp: CurrentPrice, bargainer: Bargainer) -> PriceChange:
        if bargainer.order.direction == OrderDirection.BUY:
            # 小数的相减是有误差的，比如32.92-0.01=32.910000000000004
            if round(cp.bid_price + self.delta, 2) > bargainer.last_price():
                new_price = round(cp.bid_price + self.delta, 2)
                return PriceChange(cp.visible_time, bargainer.last_price(), new_price, cp)
        elif bargainer.order.direction == OrderDirection.SELL:
            if round(cp.ask_price - self.delta, 2) < bargainer.last_price():
                new_price = round(cp.ask_price - self.delta, 2)
                return PriceChange(cp.visible_time, bargainer.last_price(), new_price, cp)
        return None

    def __init__(self, delta) -> None:
        super().__init__()
        self.delta = delta


class AbstractAccount(metaclass=ABCMeta):

    def __init__(self, name: str, initial_cash: float):
        self.name = name
        self.cash = initial_cash
        self.initial_cash = initial_cash
        self.positions = {}
        self.history_net_value: Dict[Timestamp, float] = {}
        self.order_status_callback = None
        # 记录该账户自初始化后新下的订单
        self.new_placed_orders: List[Order] = []
        self.real_order_id_to_order: Dict[str, Order] = {}

    def get_order_by_real_order_id(self, real_order_id: str):
        try:
            return self.real_order_id_to_order[real_order_id]
        except KeyError:
            return None

    @abstractmethod
    def match(self, data):
        """
        撮合订单，只有回测账户需要实现
        :param data:
        :return:
        """
        pass

    def get_new_placed_orders(self) -> List[Order]:
        """
        获取当前账户
        :return:
        """
        return self.new_placed_orders

    def get_new_placed_open_orders(self) -> List[Order]:
        res = []
        for o in self.new_placed_orders:
            if o.status in [OrderStatus.CREATED, OrderStatus.PARTIAL_FILLED, OrderStatus.SUBMITTED]:
                res.append(o)
        return res

    def with_order_callback(self, order_callback: OrderStatusCallback):
        self.order_status_callback = order_callback
        return self

    @do_log(target_name='下单', escape_params=[EscapeParam(index=0, key='self')], split=True)
    @alarm(target='下单', escape_params=[EscapeParam(index=0, key='self')])
    def place_order(self, order: Order):
        try:
            order.with_status_callback(self.order_status_callback).with_account(self.name)
            self.do_place_order(order)
            self.new_placed_orders.append(order)

            # 由于订单拒绝消息是通过streamer异步推送过来的，所以这里尝试等待1s
            if isinstance(self, BacktestAccount):
                pass
            else:
                for i in range(4):
                    time.sleep(0.25)
                    if order.status == OrderStatus.FAILED:
                        raise RuntimeError("下单失败，原因:{}".format(order.failed_reason))
                    # 如果是市价单的话，订单很可能已经成交了
                    if order.status == OrderStatus.FILLED:
                        return
            order.submitted()
            if isinstance(order, LimitOrder) and order.bargainer:
                order.bargainer.start_bargin()

        except Exception as e:
            # import traceback
            # logging.error("{}".format(traceback.format_exc()))
            order.failed(str(e.args))
            raise e

    @abstractmethod
    def do_place_order(self, order: Order):
        """
        由子类实现，约定为如果成功，则方法正常结束，如果失败则抛出异常
        :param order:
        :return:
        """
        pass

    @do_log(target_name='取消订单', escape_params=[EscapeParam(index=0, key='self')])
    @alarm(target='取消订单', escape_params=[EscapeParam(index=0, key='self')])
    def cancel_order(self, order: Order, reason: str):
        try:
            self.do_cancel_order(order)
            order.cancelled(reason)
        except Exception as e:
            raise e

    @abstractmethod
    def do_cancel_order(self, order: Order):
        """
        由子类实现，约定为如果成功，则方法正常结束，如果失败则抛出异常
        :param order:
        :return:
        """
        pass

    @do_log(target_name='更新订单', escape_params=[EscapeParam(index=0, key='self')])
    @alarm(target='更新订单', escape_params=[EscapeParam(index=0, key='self')])
    def update_order_price(self, order, new_price: float):
        if not isinstance(order, LimitOrder):
            raise RuntimeError("只适用于限价单")
        try:
            self.do_update_order_price(order, new_price)
        except Exception as e:
            raise e

    @abstractmethod
    def do_update_order_price(self, order, new_price):
        """
        由子类实现，约定为如果成功，则方法正常结束，如果失败则抛出异常
        :param new_price:
        :param order:
        :return:
        """
        pass

    def order_filled(self, order: Order, executions: List[Execution], replaced=False):
        pre_cash_cost = order.cash_cost()
        pre_position = order.filled_quantity if order.direction == OrderDirection.BUY else -order.filled_quantity
        pre_order_status = order.status

        if not replaced:
            for execution in executions:
                order.order_filled(execution)
        else:
            order.replace_order_filled(executions)

        after_cash_cost = order.cash_cost()
        after_position = order.filled_quantity if order.direction == OrderDirection.BUY else -order.filled_quantity

        self.cash -= after_cash_cost - pre_cash_cost
        self.update_position(order.code, after_position - pre_position)
        self.save()
        if self.order_status_callback:
            if pre_order_status != order.status and order.status == OrderStatus.FILLED:
                self.order_status_callback.order_status_change(order)
            if pre_order_status != order.status and order.status == OrderStatus.PARTIAL_FILLED:
                self.order_status_callback.order_status_change(order)

    @abstractmethod
    def valid_scope(self, codes: List[str]):
        """
        校验code是否有效
        :param codes:
        :return:
        """
        pass

    def save(self):
        account_repo: AccountRepo = BeanContainer.getBean(AccountRepo)
        account_repo.save(self)

    def calc_net_value(self, current_prices: Mapping[str, float], current_time: Timestamp):
        net_value = 0
        net_value += self.cash
        for code in self.positions.keys():
            net_value += current_prices[code] * self.positions[code]
        self.history_net_value[current_time] = net_value

    def update_position(self, code, position_change):
        if code in self.positions:
            self.positions[code] = self.positions[code] + position_change
        else:
            self.positions[code] = position_change

        if self.positions[code] == 0:
            self.positions.pop(code)

    def net_value(self, current_prices: Dict[str, float]):
        """
        计算该账户的资产净值
        :param param:
        :return:
        """
        net_value = 0
        net_value += self.cash
        for code in self.positions.keys():
            if code not in current_prices:
                raise RuntimeError("缺少价格数据")
            net_value += self.positions[code] * current_prices[code]
        return net_value


class AccountRepo(metaclass=ABCMeta):
    @abstractmethod
    def save(self, account: AbstractAccount):
        pass

    @abstractmethod
    def find_one(self, account_name):
        pass


class OrderRepo(metaclass=ABCMeta):

    @abstractmethod
    def find_by_account_name(self, account_name):
        pass

    def save(self, order: Order):
        pass


class BacktestAccount(AbstractAccount):

    def __init__(self, name: str, initial_cash: float, data_portal: DataPortal):

        super().__init__(name, initial_cash)
        self.data_portal = data_portal

    def match(self, data):
        # 有时在一个bar的周期内，同时有多个订单成交，这多个订单之间可能存在一些约束关系，比如若其中一个订单成交就取消另一个
        # 对这种情况，由于回测环境下这多个订单的成交事件都是在bar结束的时候才发出的，没有了先后关系
        # 针对括号单的情况，如果止赢和止损都撮合成功的情况，会先成交止损单，后成交止盈单

        match_result: List[Tuple[Order, Execution]] = []
        for order in self.get_new_placed_orders():
            if order.status not in [OrderStatus.SUBMITTED, OrderStatus.PARTIAL_FILLED]:
                continue
            if isinstance(data, Bar):
                execution = order.bar_match(data)
            elif isinstance(data, CurrentPrice):
                execution = order.current_price_match(data)
            else:
                raise RuntimeError("非法的撮合数据")
            if execution:
                match_result.append((order, execution))

        if len(match_result) > 1:
            logging.warning("一个时间周期内同时成交了多笔订单，data:{}".format(data.__dict__))
        # 如果有止损单的话，先成交
        for t in match_result:
            if isinstance(t[0], StopOrder):
                self.order_filled(t[0], [t[1]])
        for t in match_result:
            if not isinstance(t[0], StopOrder):
                self.order_filled(t[0], [t[1]])

    def do_place_order(self, order: Order):
        if isinstance(order, LimitOrder) and order.bargainer:
            raise RuntimeError('回测环境中不支持议价')
        # 尝试使用当前价格进行撮合
        cp: CurrentPrice = self.data_portal.current_price([order.code], order.place_time)[order.code]
        execution = order.current_price_match(cp)
        if execution:
            self.order_filled(order, [execution])

    def do_cancel_order(self, order: Order):
        pass

    def do_update_order_price(self, order, new_price):
        raise NotImplementedError

    def valid_scope(self, codes):
        pass
