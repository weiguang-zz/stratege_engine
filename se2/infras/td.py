import asyncio
import time
from td.client import TDClient
from td.stream import TDStreamerClient
from se2.domain.account import *
import xml.etree.ElementTree as ET

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
        self.stream_message_handlers: List[AbstractMessageHandler] = [
            HeartbeatMessageHandler(),
            ResponseMessageHandler(),
            OrderFilledMessageHandler(self)
        ]
        self.start_sync_order_execution_use_stream()
        # self.start_sync_order_execution_thread()

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
        # 如果订单已成交，下面方法不会抛异常
        self.client.cancel_order(self.account_id, order.real_order_id)

    def do_update_order_price(self, order, new_price):
        """
        对于td来说，取消订单等价于取消原来的订单，然后重新下一个订单。但是在框架层面仍然要维持同一个订单
        :param new_price:
        :param order:
        :return:
        """
        if not order.real_order_id:
            raise RuntimeError("没有td订单号")
        if order.status != OrderStatus.FILLED:
            self.do_cancel_order(order)
        # 因为监听订单成交详情是在独立的线程中，所以在任何操作之前，订单状态都有可能变成成交的状态
        if order.status != OrderStatus.FILLED:
            new_order = LimitOrder(order.code, order.direction, order.quantity - order.filled_quantity,
                                   Timestamp.now(tz='Asia/Shanghai'), "更新订单", order.ideal_price,
                                   new_price)
            self.do_place_order(new_order)
            # 更新real_order_id到order的映射
            self.real_order_id_to_order.pop(order.real_order_id)
            order.set_real_order_id(new_order.real_order_id)
            self.real_order_id_to_order[new_order.real_order_id] = order

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
            lp = order.limit_price if order.limit_price else order.bargin_algo.initial_limit_price()
            td_order["price"] = round(lp, 2)
        if isinstance(order, StopOrder):
            td_order['stopPrice'] = order.stop_price

        return td_order

    def start_sync_order_execution_thread(self):
        """
        启动订单执行详情同步线程，该同步方法是通过get_order的方法来进行同步的，会有比较大的延迟
        :return:
        """

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

    def start_sync_order_execution_use_stream(self):
        stream_client: TDStreamerClient = self.client.create_streaming_session()
        stream_client.account_activity()

        # Build the Pipeline.
        async def do_sync():
            await stream_client.build_pipeline()
            while True:
                message_decoded = await stream_client.start_pipeline()
                logging.info("receive message:{}".format(message_decoded))
                for handler in self.stream_message_handlers:
                    try:
                        handler.process_message(message_decoded)
                    except:
                        import traceback
                        logging.error("处理stream消息异常:{}".format(traceback.format_exc()))

        threading.Thread(target=lambda: asyncio.run(do_sync()), name='sync_order_use_stream').start()
        # 等待3秒以完成stream初始化
        time.sleep(3)


class AbstractMessageHandler(metaclass=ABCMeta):
    @abstractmethod
    def process_message(self, message: Dict):
        pass


class OrderFilledMessageHandler(AbstractMessageHandler):

    def __init__(self, account: TDAccount):
        self.account: TDAccount = account

    """
    响应订单成交的消息
    """

    def process_message(self, message: Dict):
        if 'data' not in message:
            return
        for single_msg in message['data']:
            if single_msg['service'] != 'ACCT_ACTIVITY':
                continue
            for single_content in single_msg['content']:
                if single_content['1'] != self.account.account_id or single_content['2'] not in ['OrderFill',
                                                                                                 'OrderPartialFill']:
                    continue
                # 解析执行详情
                root: ET.Element = ET.fromstring(single_content['3'])
                order_id = root.find('{urn:xmlns:beb.ameritrade.com}Order/{urn:xmlns:beb.ameritrade.com}OrderKey').text
                order: Order = self.account.get_order_by_real_order_id(order_id)
                if not order:
                    logging.warning("订单:{}不是该账户发出的，成交详情将会忽略".format(order_id))
                else:
                    if order.status not in [OrderStatus.SUBMITTED, OrderStatus.PARTIAL_FILLED]:
                        logging.warning("订单:{}的状态不对，成交详情将会忽略".format(order.__dict__))
                    else:
                        # 处理成交
                        exec_element = root.find('{urn:xmlns:beb.ameritrade.com}ExecutionInformation')
                        t = Timestamp(exec_element.find('{urn:xmlns:beb.ameritrade.com}Timestamp').text).tz_convert(
                            'Asia/Shanghai')
                        quantity = int(exec_element.find("{urn:xmlns:beb.ameritrade.com}Quantity").text)
                        price = float(exec_element.find("{urn:xmlns:beb.ameritrade.com}ExecutionPrice").text)
                        exec_id = exec_element.find("{urn:xmlns:beb.ameritrade.com}ID").text
                        e = Execution(exec_id, 0, quantity, price, t, 0, order_id)
                        self.account.order_filled(order, [e])


class ResponseMessageHandler(AbstractMessageHandler):

    def process_message(self, message: Dict):
        if 'response' not in message:
            return
        for resp in message['response']:
            if resp['content']['code'] != 0:
                raise RuntimeError("错误的响应:{}".format(message))


class HeartbeatMessageHandler(AbstractMessageHandler):
    """
    处理heartbeat消息，会在两种情况下进行告警，一是一段时间内没有收到heartbeat消息，td的heartbeat消息会每10s给客户端发送一条
    二是heartbeat消息的延迟，每一条heartbeat消息上有带上时间戳，拿这个时间戳跟当前时间进行比较
    """

    def __init__(self, allow_delay=Timedelta(seconds=3), check_period=Timedelta(seconds=20)):
        self.last_heart_beat_time = None
        self.allow_delay = allow_delay
        self.check_period = check_period
        threading.Thread(target=self.check, name='heartbeat check').start()

    def check(self):
        # 检查最近一段时间内是否有heartbeat消息
        @alarm(level=AlarmLevel.ERROR, target='心跳检查', freq=Timedelta(minutes=10))
        def do_check():
            if self.last_heart_beat_time:
                if (Timestamp.now() - self.last_heart_beat_time) > self.check_period:
                    raise RuntimeError("一段时间内没有收到heartbeat消息")

        while True:
            try:
                do_check()
                time.sleep(10)
            except:
                import traceback
                logging.error("{}".format(traceback.format_exc()))

    @alarm(level=AlarmLevel.ERROR, target='处理心跳消息', freq=Timedelta(minutes=10),
           escape_params=[EscapeParam(index=0, key='self')])
    def process_message(self, message: Dict):
        if 'notify' not in message:
            return
        server_time = Timestamp(int(message['notify'][0]['heartbeat']), unit='ms', tz='Asia/Shanghai')
        now = Timestamp.now(tz='Asia/Shanghai')
        if (now - server_time) > self.allow_delay:
            raise RuntimeError("心跳消息延迟太高，消息时间:{},当前时间:{}".format(server_time, now))