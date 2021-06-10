from __future__ import annotations

import time
from threading import Thread

import trading_calendars

from se2.domain.account import *
from se2.domain.time_series import *


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
            if order not in [OrderStatus.SUBMITTED, OrderStatus.PARTIAL_FILLED]:
                continue
            if isinstance(data, Bar):
                execution = order.bar_match(data)
            elif isinstance(data, CurrentPrice):
                execution = order.current_price_match(data)
            else:
                raise RuntimeError("非法的撮合数据")
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
        if isinstance(order, LimitOrder) and order.bargin_algo:
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


class EventDefinitionType(Enum):
    TIME = 0
    DATA = 1


class Rule(metaclass=ABCMeta):

    def is_match(self, calendar: TradingCalendar, dt: Timestamp):
        if not self._next_time:
            self._next_time = self.next_time(calendar, dt)
        if dt >= self._next_time:
            self._next_time = self.next_time(calendar, dt)
            return True
        return False

    @abstractmethod
    def next_time(self, calendar: TradingCalendar, current_time: Timestamp) -> Timestamp:
        pass

    def __init__(self, minute_offset, second_offset):
        self._next_time = None
        self.minute_offset = minute_offset
        self.second_offset = second_offset


class MarketOpen(Rule):
    def next_time(self, calendar: TradingCalendar, current_time: Timestamp) -> Timestamp:
        # 因为TradingCalendar默认的开盘时间是开盘后一分钟，所以这里做一下调整
        dt = calendar.next_open(current_time) + Timedelta(minutes=self.minute_offset - 1) + \
             Timedelta(seconds=self.second_offset)
        # 因为经过offset调整之后，这个时间可能在当前时间之前，比如如果minute_offset=-30, 当前时间为盘前10分钟，则下一个执行时间
        # 是下下个开盘前的10分钟
        if dt <= current_time:
            dt = calendar.next_open(calendar.next_open(current_time)) + Timedelta(minutes=self.minute_offset - 1) \
                 + Timedelta(seconds=self.second_offset)
        return dt

    def __init__(self, minute_offset=0, second_offset=0):
        super().__init__(minute_offset, second_offset)


class MarketClose(Rule):
    def next_time(self, calendar: TradingCalendar, current_time: Timestamp) -> Timestamp:
        dt = calendar.next_close(current_time) + Timedelta(minutes=self.minute_offset) + \
             Timedelta(seconds=self.second_offset)
        if dt <= current_time:
            dt = calendar.next_close(calendar.next_close(current_time)) + Timedelta(minutes=self.minute_offset) + \
                 Timedelta(seconds=self.second_offset)
        return dt

    def __init__(self, minute_offset=0, second_offset=0):
        super().__init__(minute_offset, second_offset)


class EventDefinition(object):
    def __init__(self, tp: EventDefinitionType, time_rule: Rule = None, ts_type_name: str = None,
                 order: int = 0, is_bar: bool = False):
        self.tp = tp
        self.time_rule = time_rule
        self.ts_type_name = ts_type_name
        self.order = order
        self.is_bar = is_bar

    def compareTo(self, other: EventDefinition) -> int:
        if self.order != other.order:
            return self.order - other.order
        else:
            # 数据事件要优先于时间事件
            if other.tp == EventDefinitionType.DATA:
                return -1
            return 1


class Event(object):

    def __init__(self, event_definition: EventDefinition, visible_time: Timestamp, data: object):
        self.event_definition = event_definition
        self.visible_time = visible_time
        self.data = data

    def __lt__(self, other: Event):
        if self.visible_time == other.visible_time:
            if self.event_definition.compareTo(other.event_definition) < 0:
                return True
            else:
                return False
        else:
            return self.visible_time < other.visible_time

    def __str__(self):
        return '[Event]: event_definition:{ed}, visible_time:{visible_time}, data:{data}'. \
            format(ed=self.event_definition, visible_time=self.visible_time, data=self.data)


class Scope(object):
    """
    策略的作用域，codes是全局资产列表，trading_calendar是策略使用的交易所日历，这个日历用来生成时间事件
    """

    def __init__(self, codes: List[str], trading_calendar: TradingCalendar):
        self.codes = codes
        self.trading_calendar = trading_calendar


class EventSubscriber(metaclass=ABCMeta):
    @abstractmethod
    def on_event(self, event: Event):
        pass


class TimeEventThread(Thread):
    def __init__(self, subscriber: EventSubscriber, time_event_conditions: List[EventDefinition],
                 calendar: TradingCalendar):
        super().__init__()
        self.name = "time_event_thread"
        self.subscriber = subscriber
        for ed in time_event_conditions:
            if not ed.tp == EventDefinitionType.TIME:
                raise RuntimeError("wrong event definition type")
        self.time_event_conditions = time_event_conditions
        self.calendar = calendar

    def run(self) -> None:
        while True:
            try:
                t: Timestamp = Timestamp.now(tz='Asia/Shanghai')
                logging.info("当前时间:{}".format(t))
                for ed in self.time_event_conditions:
                    if ed.time_rule.is_match(self.calendar, t):
                        event = Event(ed, t, {})
                        self.subscriber.on_event(event)
                time.sleep(1)
            except:
                import traceback
                logging.error("{}".format(traceback.format_exc()))


class EventProducer(TimeSeriesSubscriber):
    """
    所有事件都由事件产生器产生，它要生成的事件由事件定义来定义，系统支持两种类型的事件，一类是时间事件，这类事件的定义
    是基于规则产生的。另一类是数据事件，这类事件是通过时序类型生成的，时序类型有两种，一种是实时的，另一种是历史的，他们都可以用来产生数据事件
    """

    def on_data(self, data: TSData):
        ed = self.ts_type_name_to_ed[data.ts_type_name]
        if not ed:
            raise RuntimeError("wrong ts type")
        self.subscriber.on_event(Event(event_definition=ed, visible_time=data.visible_time, data=data))

    def history_events(self, scope: Scope, start: Timestamp, end: Timestamp) -> List[Event]:
        total_events = []

        # 组装时间事件
        if len(self.time_event_definitions) > 0:

            delta = Timedelta(minutes=1)
            p = start
            while p <= end:
                for ed in self.time_event_definitions:
                    if ed.time_rule.second_offset != 0:
                        raise RuntimeError("回测过程中的时间事件不允许秒级偏移")
                    if ed.time_rule.is_match(scope.trading_calendar, p):
                        total_events.append(Event(ed, p, {}))

                p += delta

        # 组装数据事件
        if len(self.data_event_definitions) > 0:

            for ed in self.data_event_definitions:
                ts = BeanContainer.getBean(TimeSeriesRepo).find_one(ed.ts_type_name)
                command = HistoryDataQueryCommand(start, end, scope.codes)
                command.with_calendar(scope.trading_calendar)
                df = ts.history_data(command, from_local=True)
                for (visible_time, code), values in df.iterrows():
                    data: Dict = values.to_dict()
                    data['code'] = code
                    if ed.is_bar:
                        data = Bar(ed.ts_type_name, visible_time, code, values.to_dict())
                    event = Event(ed, visible_time, data)
                    total_events.append(event)

        return total_events

    def subscribe(self, subscriber: EventSubscriber):
        self.subscriber = subscriber

    def start(self, scope: Scope):
        for ed in self.data_event_definitions:
            ts = BeanContainer.getBean(TimeSeriesRepo).find_one(ed.ts_type_name)
            ts.subscribe(self, scope.codes)

        TimeEventThread(self.subscriber, self.time_event_definitions, scope.trading_calendar).start()

    def __init__(self, event_definitions: List[EventDefinition]):
        self.event_definitions = event_definitions
        self.subscriber = None
        self.time_event_definitions = [ed for ed in self.event_definitions if ed.tp == EventDefinitionType.TIME]
        self.data_event_definitions = [ed for ed in self.event_definitions if ed.tp == EventDefinitionType.DATA]
        self.ts_type_name_to_ed = \
            {ed.ts_type_name: ed for ed in event_definitions if ed.tp == EventDefinitionType.DATA}


class AbstractStrategy(OrderStatusCallback, metaclass=ABCMeta):
    """
    所有策略都应该继承这个基类， 在do_initialize方法中注册自己的事件定义以及回调。通过do_order_status_change来响应订单状态变更事件
    """

    @do_log(target_name="订单状态变更", escape_params=[EscapeParam(index=0, key='self')])
    def order_status_change(self, order):
        self.do_order_status_change(order)

    @abstractmethod
    def do_order_status_change(self, order):
        pass

    # def ensure_order_filled_v2(self, order: LimitOrder, duration: int, delta=0.01):
    #     """
    #     在duration所指定的时间范围内，跟踪市场上的买一和卖一的价格。 超过duration之后，则挂市价单
    #     :param delta:
    #     :param order:
    #     :param duration:
    #     :return:
    #     """
    #
    #     def do_ensure():
    #         try:
    #             now = Timestamp.now(tz='Asia/Shanghai')
    #             threshold = now + Timedelta(seconds=duration)
    #             while now <= threshold:
    #                 if (order.status == OrderStatus.CANCELED) or (
    #                         order.status == OrderStatus.FILLED) or order.status == OrderStatus.FAILED:
    #                     logging.info("没有为成交的订单，不需要ensure")
    #                     break
    #                 current_price = None
    #                 try:
    #                     current_price: CurrentPrice = self.data_portal.current_price([order.code])[order.code]
    #                 except:
    #                     pass
    #                 if current_price:
    #                     if order.direction == OrderDirection.BUY:
    #                         target_price = current_price.bid_price + delta
    #                         # 乘以1.1是为了防止跟自己的出价进行比较
    #                         if abs(target_price - order.limit_price) > delta * 1.1:
    #                             self.account.update_order_price(order, target_price, current_price)
    #                     else:
    #                         target_price = current_price.ask_price - delta
    #                         if abs(target_price - order.limit_price) > delta * 1.1:
    #                             self.account.update_order_price(order, target_price, current_price)
    #                 time.sleep(0.1)
    #                 now = Timestamp.now(tz='Asia/Shanghai')
    #             if order.status == OrderStatus.CREATED or order.status == OrderStatus.SUBMITTED or \
    #                     order.status == OrderStatus.PARTIAL_FILLED:
    #                 logging.info("订单在规定时间内没有成交，将会使用市价单挂单")
    #                 self.account.cancel_order(order, "没有在规定时间内成交")
    #                 new_order = MKTOrder(order.code, order.direction, int(order.quantity - order.filled_quantity), now,
    #                                      order.reason)
    #                 self.account.place_order(new_order)
    #                 # 一般情况下，市价单会立即成交，但是存在某个资产不可卖空的情况，这种情况下，市价单可能会被保留
    #                 # 针对这种情况，需要取消掉，以免在不合适的时候成交
    #                 time.sleep(10)
    #                 if new_order.status == OrderStatus.SUBMITTED or new_order.status == OrderStatus.PARTIAL_FILLED:
    #                     logging.info("市价单没有在规定的时间内成交，猜想可能是因为没有资产不可卖空导致的，将会取消订单")
    #                     self.account.cancel_order(new_order, "订单没有在规定时间内成交，可能是不可卖空")
    #         except:
    #             import traceback
    #             err_msg = "ensure order filled失败:{}".format(traceback.format_exc())
    #             logging.error(err_msg)
    #             # 显示告警，因为在线程的Runable方法中，不能再抛出异常。
    #             do_alarm('ensure order filled', AlarmLevel.ERROR, None, None, '{}'.format(traceback.format_exc()))
    #
    #     if not isinstance(order, LimitOrder):
    #         raise RuntimeError("wrong order type")
    #
    #     threading.Thread(name='ensure_order_filled', target=do_ensure).start()

    @abstractmethod
    def do_initialize(self):
        pass

    def initialize(self, data_portal: DataPortal, account: AbstractAccount, engine: Engine):
        self.account = account
        self.data_portal = data_portal
        self.engine = engine
        self.do_initialize()

    def __init__(self, scope: Scope):
        self.scope = scope
        self.account: AbstractAccount = None
        self.data_portal: DataPortal = None
        self.engine: Engine = None

    def start_check_realtime_data_thread(self):
        def do_check(code: str):
            if 'USD' in code:
                # 如果是盘前，允许有30分钟的延迟
                # 如果是盘中，允许有1分钟的延迟
                now = Timestamp.now(tz='Asia/Shanghai')
                us_calendar: TradingCalendar = trading_calendars.get_calendar("NYSE")
                pre_open_last = Timedelta(minutes=30, hours=5)
                pre_open_allowed_delay = Timedelta(minutes=30)
                market_open_allowed_delay = Timedelta(minutes=1)

                next_close = us_calendar.next_close(now)
                previous_open = us_calendar.previous_open(next_close)
                pre_open_start = previous_open - pre_open_last
                if pre_open_start + pre_open_allowed_delay < now < previous_open:
                    try:
                        self.data_portal.current_price([code], now, pre_open_allowed_delay)
                    except:
                        import traceback
                        logging.error("{}".format(traceback.format_exc()))
                elif previous_open < now < next_close:
                    try:
                        self.data_portal.current_price([code], now, market_open_allowed_delay)
                    except:
                        import traceback
                        logging.error("{}".format(traceback.format_exc()))

        def check_realtime_data():
            while True:
                try:
                    logging.info("开始检查实时数据")
                    for code in self.scope.codes:
                        do_check(code)
                except:
                    import traceback
                    err_msg = "检查实时数据异常:{}".format(traceback.format_exc())
                    logging.error(err_msg)
                time.sleep(10 * 60)

        threading.Thread(name="check_realtime_data", target=check_realtime_data).start()


class DataPortal(object):
    """
    策略要获取实时价格和历史价格，都需要通过数据面板。在回测中获取实时数据其实是获取的历史某个时间点的数据，在实盘中获取实时数据是真正的实时数据。
    """

    def history_data(self, ts_type_name, command: HistoryDataQueryCommand):
        ts = BeanContainer.getBean(TimeSeriesRepo).find_one(ts_type_name)
        return ts.history_data(command)

    def __init__(self, is_backtest: bool, ts_type_name_for_current_price: str,
                 subscribe_codes: List[str] = None):
        self.ts_type_name_for_current_price = ts_type_name_for_current_price
        self.is_backtest = is_backtest
        self.subscribe_codes = subscribe_codes
        ts_repo: TimeSeriesRepo = BeanContainer.getBean(TimeSeriesRepo)
        self.current_price_ts: TimeSeries = ts_repo.find_one(ts_type_name_for_current_price)
        if not is_backtest:
            if not subscribe_codes:
                raise RuntimeError("need subscribe codes")
            self.current_price_ts.subscribe(None, subscribe_codes)

    def current_price(self, codes: List[str], current_time: Timestamp = None) -> Mapping[str, CurrentPrice]:
        """
        若在实盘环境下，current_time参数会被丢弃
        :param delay_allowed:
        :param codes:
        :param current_time:
        :return:
        """
        if not self.is_backtest:
            current_time = None

        ret = self.current_price_ts.current_price(codes, current_time)

        # # 检查数据是否缺失或者有延迟
        # for code in codes:
        #     if code not in ret:
        #         raise RuntimeError("没有获取到{}的最新价格，当前时间:{}".format(code, current_time))
        #     cp = ret[code]
        #     if delay_allowed:
        #         if current_time - cp.visible_time > delay_allowed:
        #             raise RuntimeError("没有获取到{}的最新价格，当前时间:{}, 最新价格:{}, 允许的延迟:{}".
        #                                format(code, current_time, cp.__dict__, delay_allowed))
        return ret


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


class Engine(EventSubscriber):
    """
    引擎负责驱动策略运行，可以在回测环境中，也可以在实盘环境中。 它实现的方式就是通过拿到策略定义的事件定义，交给事件产生器来生成事件，然后把
    事件交给策略。 在回测环境中是预先生成所有的事件，在实盘的时候是通过异步回调的方式，由时序类型 -> 事件产生器 -> 引擎 -> 策略。
    """

    def register_event(self, event_definition: EventDefinition,
                       callback: Callable[[Event], None]):
        if event_definition in self.callback_map:
            raise RuntimeError("wrong event definition")
        self.callback_map[event_definition] = callback
        self.event_definitions.append(event_definition)

    def on_event(self, event: Event):
        """
        接收实时事件
        :param event:
        :return:
        """
        callback = self.callback_for(event.event_definition)
        callback(event)

    def match(self, event: Event):
        if not (isinstance(event.data, Bar) or isinstance(event.data, CurrentPrice)):
            raise RuntimeError("wrong event data")
        self.account.match(event.data)

    def calc_net_value(self, event: Event):
        if len(self.account.positions) > 0:
            codes = list(self.account.positions.keys())
            cps: Mapping[str, CurrentPrice] = self.data_portal.current_price(codes, event.visible_time, None)
            price_map = {code: cps[code].price for code in cps.keys()}
            self.account.calc_net_value(price_map, event.visible_time)

    def run_backtest(self, strategy: AbstractStrategy, start: Timestamp, end: Timestamp,
                     ts_type_name_for_match: str):
        strategy.initialize(self.data_portal, self.account, self)
        self.account.with_order_callback(strategy)

        # 注册时间事件回调用于计算账户净值
        self.register_event(EventDefinition(tp=EventDefinitionType.TIME, time_rule=MarketClose(), order=1000),
                            self.calc_net_value)
        # 注册事件回调用于撮合订单
        self.register_event(
            EventDefinition(tp=EventDefinitionType.DATA, ts_type_name=ts_type_name_for_match, order=-10, is_bar=True),
            self.match)

        event_line = EventLine()

        ep = EventProducer(self.event_definitions)
        event_line.add_all(ep.history_events(strategy.scope, start, end))

        event: Event = event_line.pop_event()
        while event is not None:
            callback = self.callback_for(event.event_definition)
            try:
                callback(event)
            except:
                import traceback
                logging.error("{}".format(traceback.format_exc()))

            event = event_line.pop_event()
        self.account.save()
        return self.account

    def run(self, strategy: AbstractStrategy):
        strategy.initialize(self.data_portal, self.account, self)
        self.account.with_order_callback(strategy)

        self.register_event(EventDefinition(tp=EventDefinitionType.TIME, time_rule=MarketClose(second_offset=10)),
                            self.calc_net_value)

        ep = EventProducer(self.event_definitions)
        ep.subscribe(self)
        ep.start(strategy.scope)

    def __init__(self, account: AbstractAccount, data_portal: DataPortal):
        self.callback_map = {}
        self.event_definitions = []

        self.account = account
        self.data_portal = data_portal

    def callback_for(self, event_definition: EventDefinition):
        return self.callback_map[event_definition]
