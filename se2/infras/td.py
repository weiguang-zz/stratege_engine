from td.client import TDClient

from se2.domain.account import *

client: TDClient = None


def initialize(client_id, redirect_uri, credentials_path):
    global client
    if client:
        raise RuntimeError("client已经初始化了")
    client = TDClient(
        client_id=client_id,
        redirect_uri=redirect_uri,
        credentials_path=credentials_path
    )
    client.login()


class TDAccount(AbstractAccount):

    def match(self, data):
        raise NotImplementedError

    def __init__(self, name: str, initial_cash: float, account_id: str):
        super().__init__(name, initial_cash)
        self.account_id = account_id
        self.client: TDClient = client
        self.start_sync_order_execution_thread()

    @alarm(level=AlarmLevel.ERROR, target="同步订单成交情况", escape_params=[EscapeParam(index=0, key='self')])
    def sync_order_execution(self, order: Order):
        """
        同步订单状态以及订单执行情况
        :return:
        """
        if not order.real_order_id:
            raise RuntimeError("没有td订单号")
        o: dict = self.client.get_orders(self.account_id, order.real_order_id)
        if o.get("status") == 'CANCELED':
            raise RuntimeError("订单在服务端已经被取消")

        td_executions: List[Dict] = o.get("orderActivityCollection")
        executions: List[Execution] = []
        if td_executions and len(td_executions) > 0:
            idx = 0
            for td_execution in td_executions:
                if not td_execution.get("executionType") == 'FILL':
                    raise NotImplementedError
                td_execution_legs: List[Dict] = td_execution.get("executionLegs")
                if len(td_execution_legs) > 1:
                    raise NotImplementedError
                elif len(td_execution_legs) == 1:
                    td_execution_leg = td_execution_legs[0]
                    single_filled_quantity = td_execution_leg.get("quantity")
                    single_filled_price = td_execution_leg.get('price')
                    exec = Execution(str(idx), 0, single_filled_quantity, single_filled_price,
                                     Timestamp(td_execution_leg.get("time"), tz='UTC'), 0, order.real_order_id)
                    executions.append(exec)
                    idx += 1
            self.order_filled(order, executions, replaced=True)

    @retry(limit=3)
    def do_place_order(self, order: Order):
        td_order: Dict = self.change_to_td_order(order)
        # 如果下单失败，下面方法会抛异常
        resp = self.client.place_order(self.account_id, td_order)
        td_order_id = resp.get('order_id')
        order.set_real_order_id(td_order_id)

    @retry(limit=3)
    def do_cancel_order(self, order: Order):
        if not order.real_order_id:
            raise RuntimeError("没有td订单号")
        if order.status == OrderStatus.CANCELED:
            return
        # 下面方法如果失败会抛异常
        self.client.cancel_order(self.account_id, order.real_order_id)

    def do_update_order_price(self, order):
        """
        对于td来说，取消订单等价于取消原来的订单，然后重新下一个订单。但是在框架层面仍然要维持同一个订单
        :param order:
        :return:
        """
        if not order.real_order_id:
            raise RuntimeError("没有td订单号")
        self.do_cancel_order(order)
        new_order = LimitOrder(order.code, order.direction, order.quantity - order.filled_quantity,
                               Timestamp.now(tz='Asia/Shanghai'), "更新订单",
                               order.limit_price)
        self.do_place_order(new_order)
        order.set_real_order_id(new_order.real_order_id)

    def valid_scope(self, codes):
        for code in codes:
            cc = code.split("_")
            symbol = cc[0]
            symbol_type = cc[1]
            if symbol_type != 'STK':
                raise NotImplementedError
            res = self.client.search_instruments(symbol, "symbol-search")
            if not res or len(res) <= 0 or symbol not in res:
                raise RuntimeError("没有查询到资产,code:" + code)
            if res[symbol]['assetType'] != 'EQUITY':
                raise RuntimeError("资产不是股票类型，暂不支持")

    def change_to_td_order(self, order: Order) -> Dict:
        if isinstance(order, MKTOrder):
            order_type = "MARKET"
        elif isinstance(order, LimitOrder):
            order_type = "LIMIT"
        elif isinstance(order, StopOrder):
            order_type = "STOP"
        else:
            raise NotImplementedError

        if order.extended_time:
            session = "SEAMLESS"
        else:
            session = "NORMAL"

        # 默认为日内订单
        duration = "DAY"
        # 仅支持简单订单类型
        order_strategy_type = "SINGLE"

        # 下面构造order_leg
        if order.direction == OrderDirection.BUY:
            instruction = "BUY"
        elif order.direction == OrderDirection.SELL:
            instruction = "SELL"
        else:
            raise NotImplementedError
        cc = order.code.split('_')
        symbol = cc[0]
        # 目前仅支持股票类型
        if cc[1] == 'STK':
            asset_type = "EQUITY"
        else:
            raise NotImplementedError
        instrument = {
            "symbol": symbol,
            "assetType": asset_type
        }

        order_leg = {
            "instruction": instruction,
            "quantity": order.quantity,
            "instrument": instrument
        }
        # 构造order_leg结束

        td_order = {
            "orderType": order_type,
            "session": session,
            "duration": duration,
            "orderStrategyType": order_strategy_type,
            "orderLegCollection": [order_leg]
        }
        if isinstance(order, LimitOrder):
            td_order["price"] = order.limit_price
        if isinstance(order, StopOrder):
            td_order['stopPrice'] = order.stop_price

        return td_order

    def start_sync_order_execution_thread(self):
        def do_sync():
            while True:
                for order in self.get_new_placed_orders():
                    if order.status not in [OrderStatus.SUBMITTED, OrderStatus.PARTIAL_FILLED]:
                        continue
                    try:
                        self.sync_order_execution(order)
                    except:
                        import traceback
                        logging.error("同步订单执行详情失败{}".format(traceback.format_exc()))
                import time
                time.sleep(0.5)

        threading.Thread(target=do_sync, name="sync td orders").start()
