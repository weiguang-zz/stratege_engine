import logging

from se.domain2.account.account import AbstractAccount, MKTOrder, OrderDirection, LimitOrder
from se.domain2.domain import send_email
from se.domain2.engine.engine import AbstractStrategy, Engine, EventDefinition, EventDefinitionType, MarketOpen, \
    MarketClose, Event, DataPortal


class TestStrategy3(AbstractStrategy):
    """
    在每天收盘的时候， 如果有持仓，则平仓，并且以收盘价卖空
    在每天开盘的时候，如果有持仓(一定是空仓)，则平仓， 并且判断昨天收盘到今天开盘的涨幅是否小于0， 若是，则已开盘价买入
    交易标的： GSX
    """

    def do_initialize(self, engine: Engine):
        if engine.is_backtest:
            market_close = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketClose())
            market_open = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketOpen())
        else:
            # 实盘的时候，在收盘前5s的时候提交订单
            market_open = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketOpen(second_offset=5))
            market_close = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketClose(second_offset=-30))
        engine.register_event(market_open, self.market_open)
        engine.register_event(market_close, self.market_close)
        self.last_close_price = None
        self.is_backtest = engine.is_backtest
        self.code = self.scope.codes[0]

    def market_open(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
        dest_position = 0
        current_position = 0
        net_value = None

        current_price = self.get_recent_price_after([self.code], event.visible_time, data_portal)

        if not current_price:
            msg = "没有获取到当天的开盘价{}".format(self.code)
            logging.error(msg)
            send_email("ERROR", msg)
        else:
            net_value = account.net_value({self.code: current_price})

        if self.last_close_price and current_price and current_price < self.last_close_price:
            dest_position = int(net_value / current_price)

        if len(account.positions) > 0:
            current_position = account.positions[self.code]

        change = dest_position - current_position

        if change != 0:
            direction = OrderDirection.BUY if change > 0 else OrderDirection.SELL
            if current_price:
                order = LimitOrder(self.code, direction, abs(change), event.visible_time, current_price)
            else:
                logging.warning("没有获取到当前价格，将会以市价单下单")
                order = MKTOrder(self.code, direction, abs(change), event.visible_time)
            account.place_order(order)
            msg = "时间:{}, 当前持仓:{}, 总市值：{}, 目标持仓:{}, 昨日收盘价:{}, 今日开盘价：{}, 订单:{}" \
                .format(event.visible_time, current_position, net_value, dest_position, self.last_close_price,
                        current_price, order.__dict__)
            logging.info("开盘下单:{}".format(msg))
            send_email('[订单]', msg)
        else:
            msg = "不需要下单, 时间:{}, 当前持仓:{}, 总市值：{}, 目标持仓:{}, 昨日收盘价:{}, 今日开盘价：{}". \
                format(event.visible_time, current_position, net_value, dest_position, self.last_close_price,
                       current_price)
            logging.info(msg)

    def market_close(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
        dest_position = 0
        current_position = 0
        net_value = None

        current_price = self.get_recent_price_after([self.code], event.visible_time, data_portal)

        if not current_price:
            msg = "没有获取到当天的开盘价{}".format(self.code)
            logging.error(msg)
            send_email("ERROR", msg)
        else:
            net_value = account.net_value({self.code: current_price})
            dest_position = -int(net_value / current_price)

        if len(account.positions) > 0:
            current_position = account.positions[self.code]
        change = dest_position - current_position

        if change != 0:
            direction = OrderDirection.BUY if change > 0 else OrderDirection.SELL
            if current_price:
                order = LimitOrder(self.code, direction, abs(change), event.visible_time, current_price)
            else:
                logging.warning("没有获取到当前价格，将会以市价单下单")
                order = MKTOrder(self.code, direction, abs(change), event.visible_time)
            account.place_order(order)
            msg = "时间:{}, 当前持仓:{}, 总市值：{}, 目标持仓:{}, 当前价格, 订单:{}" \
                .format(event.visible_time, current_position, net_value, dest_position,
                        current_price, order.__dict__)
            logging.info("收盘下单:{}".format(msg))
            send_email('[订单]', msg)
        else:
            msg = "不需要下单, 时间:{}, 当前持仓:{}, 总市值：{}, 目标持仓:{}, 当前价格:{}". \
                format(event.visible_time, current_position, net_value, dest_position,
                       current_price)
            logging.info(msg)
        self.last_close_price = current_price

    def order_status_change(self, order, account):
        logging.info("订单状态变更, 订单状态:{}，账户持仓:{}, 账户现金:{}".
                     format(order.__dict__, account.positions, account.cash))
        msg = {"positions": account.positions, "cash": account.cash, "order": order.__dict__}
        send_email("【订单】成交", str(msg))
