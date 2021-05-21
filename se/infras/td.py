from __future__ import annotations

import threading

from td.client import TDClient

from se.domain2.account.account import *
from se.domain2.account.account import AbstractAccount, Order
from se.domain2.monitor import retry


class TDOrder(object):

    def __init__(self, order: Order, order_callback: OrderCallback, account: TDAccount):
        order_type = None
        if isinstance(order, MKTOrder):
            order_type = TDOrderType.MARKET
        elif isinstance(order, LimitOrder):
            order_type = TDOrderType.LIMIT
        else:
            raise NotImplementedError
        if not 'STK' in order.code:
            raise NotImplementedError

        instruction = None
        if order.direction == OrderDirection.BUY:
            instruction = TDInstruction.BUY
        elif order.direction == OrderDirection.SELL:
            instruction = TDInstruction.SELL

        cc = order.code.split('_')
        symbol = cc[0]
        asset_type = None
        if cc[1] == 'STK':
            asset_type = TDAssetType.EQUITY
        else:
            raise NotImplementedError
        instrument = TDInstrument(symbol, asset_type)
        order_leg = TDOrderLeg(instruction, order.quantity, instrument)
        self.order_type = order_type
        self.session = TDSession.NORMAL
        self.limit_price = round(order.limit_price, 2)
        self.duration = TDDuration.DAY
        self.order_strategy_type = TDOrderStrategyType.SINGLE
        self.order_legs = [order_leg]
        self.looped = True

        self.td_order_id = None
        self.framework_order: Order = order
        self.order_callback = order_callback
        self.account = account

    def to_dict(self):
        return {
            "orderType": self.order_type.name,
            "session": self.session.name,
            "price": self.limit_price,
            "duration": self.duration.name,
            "orderStrategyType": self.order_strategy_type.name,
            "orderLegCollection": [ol.to_dict() for ol in self.order_legs]
        }

    def place_order(self):
        try:
            resp = self.account.client.place_order(self.account.account_id, self.to_dict())
        except Exception as e:
            self.framework_order.status = OrderStatus.FAILED
            raise e
        self.td_order_id = resp.get('order_id')
        self.framework_order.td_order_id = self.td_order_id
        self.sync_order()
        threading.Thread(target=self.do_loop_sync, name="sync td order").start()

    def do_loop_sync(self):
        while True and self.looped:
            try:
                self.sync_order()
                import time
                time.sleep(0.5)
            except:
                import traceback
                logging.error("同步订单详情报错： {}".format(traceback.format_exc()))

            if self.framework_order.status in [OrderStatus.FAILED, OrderStatus.CANCELED, OrderStatus.FILLED]:
                logging.info("结束订单同步线程")
                break

    def sync_order(self):
        """
        同步订单状态以及订单执行情况
        :return:
        """
        o: dict = self.account.client.get_orders(self.account.account_id, self.td_order_id)
        td_order_status: str = o.get('status')
        if td_order_status in ['ACCEPTED', 'WORKING', 'QUEUED'] and self.framework_order.status == OrderStatus.CREATED:
            self.framework_order.status = OrderStatus.SUBMITTED
        elif 'PENDING' in td_order_status:
            # pending状态下，不改变状态
            pass
        elif td_order_status == "CANCELED" and self.framework_order.status != OrderStatus.CANCELED:
            self.framework_order.status = OrderStatus.CANCELED
            self.order_callback.order_status_change(self.framework_order, None)
        elif td_order_status == 'FILLED':
            # filled的状态在account.order_filled中变更
            pass
        else:
            raise NotImplementedError("非法的订单状态:" + td_order_status)

        # 同步执行详情
        if self.framework_order.status != OrderStatus.FILLED:
            executions: List[Dict] = o.get("orderActivityCollection")
            if executions and len(executions) > 0:
                filled_quantity = 0
                total_cost = 0
                for execution in executions:
                    if not execution.get("executionType") == 'FILL':
                        raise NotImplementedError
                    execution_legs: List[Dict] = execution.get("executionLegs")
                    if len(execution_legs) > 1:
                        raise NotImplementedError
                    if execution_legs and len(execution_legs) == 1:
                        execution_leg = execution_legs[0]
                        single_filled_quantity = execution_leg.get("quantity")
                        single_filled_price = execution_leg.get('price')
                        total_cost += single_filled_price * single_filled_quantity
                        filled_quantity += single_filled_quantity
                filled_avg_price = total_cost / filled_quantity
                if len(self.framework_order.execution_map) > 0:
                    old_version = self.framework_order.execution_map.get("default").version
                    oe = OrderExecution("default", old_version + 1, 0, filled_quantity, filled_avg_price, None, None,
                                        self.framework_order.direction, None)
                else:
                    oe = OrderExecution("default", 1, 0, filled_quantity, filled_avg_price, None, None,
                                        self.framework_order.direction, None)
                self.account.order_filled(self.framework_order, oe)

    def cancel(self):
        if self.framework_order.status == OrderStatus.CANCELED:
            return
        self.account.client.cancel_order(self.account.account_id, self.td_order_id)
        self.sync_order()
        # 强制设置为canceled，因为发现被取消的订单，同步过来的状态可能是QUEUED
        self.framework_order.status = OrderStatus.CANCELED
        self.looped = False
        self.order_callback.order_status_change(self.framework_order, None)


class TDOrderLeg(object):
    def __init__(self, instruction: TDInstruction, quantity, instrument: TDInstrument):
        self.instruction = instruction
        self.quantity = quantity
        self.instrument = instrument

    def to_dict(self):
        return {
            "instruction": self.instruction.name,
            "quantity": self.quantity,
            "instrument": self.instrument.to_dict()
        }


class TDInstrument(object):
    def __init__(self, symbol, asset_type: TDAssetType):
        self.symbol = symbol
        self.asset_type = asset_type

    def to_dict(self):
        return {
            "symbol": self.symbol,
            "assetType": self.asset_type.name
        }


class TDOrderType(Enum):
    LIMIT = 0
    MARKET = 1
    STOP = 2
    STOP_LIMIT = 3
    TRAILING_STOP = 4
    NET_DEBIT = 5


class TDOrderStrategyType(Enum):
    SINGLE = 0
    TRIGGER = 1
    OCO = 2


class TDSession(Enum):
    NORMAL = 0
    SEAMLESS = 1


class TDDuration(Enum):
    DAY = 0
    GOOD_TILL_CANCEL = 1


class TDInstruction(Enum):
    BUY = 0
    SELL = 1
    BUY_TO_COVER = 2
    SELL_SHORT = 3
    # for option
    BUY_TO_OPEN = 4
    BUY_TO_CLOSE = 5
    SELL_TO_OPEN = 6
    SELL_TO_CLOSE = 7


class TDAssetType(Enum):
    EQUITY = 0
    OPTION = 1


class TDAccount(AbstractAccount):

    def __init__(self, name: str, initial_cash: float):
        super().__init__(name, initial_cash)

        self.account_id = None
        self.client: TDClient = None
        self.td_orders: Mapping[str, TDOrder] = {}

    def with_client(self, client_id, redirect_uri, credentials_path,
                    account_id):
        self.account_id = account_id
        self.client = TDClient(
            client_id=client_id,
            redirect_uri=redirect_uri,
            credentials_path=credentials_path
        )
        self.client.login()

    @do_log(target_name='下单', escape_params=[EscapeParam(index=0, key='self')])
    @alarm(target='下单', escape_params=[EscapeParam(index=0, key='self')])
    @retry(limit=3)
    def place_order(self, order: Order):
        td_order = TDOrder(order, self.order_callback, self)
        td_order.place_order()
        if order.status == OrderStatus.CREATED or order.status == OrderStatus.FAILED:
            if order.status == OrderStatus.CREATED:
                self.cancel_open_order(order)
            raise RuntimeError("place order error")
        self.td_orders[td_order.td_order_id] = td_order

    def match(self, data):
        raise NotImplementedError

    @do_log(target_name='取消订单', escape_params=[EscapeParam(index=0, key='self')])
    @alarm(target='取消订单', escape_params=[EscapeParam(index=0, key='self')])
    @retry(limit=3)
    def cancel_open_order(self, open_order):
        if not open_order.td_order_id or open_order.td_order_id not in self.td_orders:
            raise RuntimeError("没有td订单号")
        self.td_orders[open_order.td_order_id].cancel()

    @do_log(target_name='更新订单', escape_params=[EscapeParam(index=0, key='self')])
    @alarm(target='更新订单', escape_params=[EscapeParam(index=0, key='self')])
    @retry(limit=3)
    def update_order(self, order: Order, reason):
        if not order.td_order_id or (order.td_order_id not in self.td_orders):
            raise RuntimeError("没有td订单号")
        if not isinstance(order, LimitOrder):
            raise NotImplementedError
        old_td_order: TDOrder = self.td_orders[order.td_order_id]
        old_td_order.cancel()
        new_order = LimitOrder(order.code, order.direction, order.quantity - order.filled_quantity,
                               Timestamp.now(tz='Asia/Shanghai'),
                               order.limit_price, None)
        new_td_order: TDOrder = TDOrder(new_order, self.order_callback, self)
        new_td_order.place_order()
        return new_order

    def start_save_thread(self):
        # 启动账户保存线程，每隔半小时会保存当前账户的操作数据
        def save():
            while True:
                try:
                    logging.info("开始保存账户数据")
                    self.save()
                except:
                    import traceback
                    err_msg = "保存账户失败:{}".format(traceback.format_exc())
                    logging.error(err_msg)
                import time
                time.sleep(30 * 60)

        threading.Thread(name="account_save", target=save).start()
