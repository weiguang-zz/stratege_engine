import logging

from se.domain2.account.account import AbstractAccount, MKTOrder, OrderDirection, LimitOrder, Order, OrderStatus
from se.domain2.domain import send_email
from se.domain2.engine.engine import AbstractStrategy, Engine, EventDefinition, EventDefinitionType, MarketOpen, \
    MarketClose, Event, DataPortal
from se.domain2.time_series.time_series import HistoryDataQueryCommand


class ACBStrategy(AbstractStrategy):
    """
    该策略会在收盘的时候，检查今日收盘是否大于今日开盘，如果大于，则做多
    会在开盘的时候，判断昨日收盘是否大于昨日开盘，如果大于，则做空
    交易标的： ACB
    回测结果：
        最大回撤:-0.18198627729641348
        胜率:0.6722222222222223
        年化夏普:5.256477163933315
        平均盈利:0.05729046297931058, 平均亏损:-0.038921799742595936

    做多两倍杠杆下的回测结果：
        最大回撤:-0.23874748346275543
        胜率:0.712707182320442
        年化夏普:5.441637490135704
        平均盈利:0.07866330194179004, 平均亏损:-0.053507280037103214
    """

    def do_order_status_change(self, order, account):
        pass

    def do_initialize(self, engine: Engine, data_portal: DataPortal):
        if engine.is_backtest:
            market_close = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketClose())
            market_open = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketOpen())
        else:
            market_open = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketOpen(second_offset=5))
            market_close = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketClose(second_offset=-60))
            market_close_set_price = EventDefinition(ed_type=EventDefinitionType.TIME,
                                                     time_rule=MarketClose())
            engine.register_event(market_close_set_price, self.set_close_price)
        engine.register_event(market_open, self.market_open)
        engine.register_event(market_close, self.market_close)
        # 初始化昨日开盘价和收盘价
        self.last_open = None
        self.last_close = None
        if not engine.is_backtest:
            command = HistoryDataQueryCommand(None, None, self.scope.codes, window=1)
            command.with_calendar(trading_calendar=self.scope.trading_calendar)
            df = data_portal.history_data("ibAdjustedDailyBar", command)
            if len(df) >= 1:
                self.last_open = df.iloc[-1]['open']
                self.last_close = df.iloc[-1]['close']
                logging.info("初始化数据成功，昨日开盘价:{}, 昨日收盘价:{}, bar的开始时间:{}"
                             .format(self.last_open, self.last_close, df.iloc[-1]['start_time']))
            else:
                raise RuntimeError("没有获取到昨日开盘价和收盘价")
            # self.last_open = 11.19
            # self.last_close = 11.03

        if len(self.scope.codes) != 1:
            raise RuntimeError("wrong codes")
        self.code = self.scope.codes[0]
        self.long_leverage = 2

    def set_close_price(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
        current_price = data_portal.current_price([self.code], event.visible_time)[self.code].price
        self.last_close = current_price
        logging.info("设置收盘价为:{}".format(current_price))

    def market_open(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
        dest_position = 0
        current_position = 0
        net_value = None

        # 获取最新的股票价格
        current_price = None
        try:
            current_price = data_portal.current_price([self.code], event.visible_time)[self.code].price
        except:
            logging.error("没有获取到当天的开盘价,code:{}".format(self.code))

        current_bid_ask = None
        try:
            current_bid_ask = data_portal.current_bid_ask([self.code])[self.code]
        except:
            logging.error("没有获取到最新的买卖价, code:{}".format(self.code))

        if current_price:
            net_value = account.net_value({self.code: current_price})

        if len(account.positions) > 0:
            current_position = account.positions[self.code]

        if current_price and self.last_open and self.last_close and self.last_close > self.last_open:
            dest_position = - int(net_value / current_price)

        change = dest_position - current_position
        if change != 0:
            direction = OrderDirection.BUY if change > 0 else OrderDirection.SELL
            reason = "时间:{}, 当前持仓:{}, 总市值：{}, 目标持仓:{}, 昨日开盘价:{}, 昨日收盘价:{}, " \
                     "今日开盘价：{}, 当前买卖价:{}, strategy:{}".format(event.visible_time, current_position, net_value,
                                                              dest_position, self.last_open, self.last_close,
                                                              current_price,
                                                              current_bid_ask.__dict__ if current_bid_ask else None,
                                                              ACBStrategy.__doc__)
            if current_bid_ask:
                delta = 0.01
                limit_price = (current_bid_ask.bid_price + delta) if direction == OrderDirection.BUY else (
                        current_bid_ask.ask_price - delta)
                order = LimitOrder(self.code, direction, abs(change), event.visible_time, limit_price)
                order.with_reason(reason)
                account.place_order(order)
                self.ensure_order_filled_v2(account, data_portal, order, duration=60, delta=delta)
                # self.ensure_order_filled(account, data_portal, order, period=60, retry_count=1)
            else:
                logging.warning("没有获取到当前价格，将会以市价单下单")
                order = MKTOrder(self.code, direction, abs(change), event.visible_time)
                order.with_reason(reason)
                account.place_order(order)
        else:
            msg = "不需要下单, 时间:{}, 当前持仓:{}, 总市值：{}, 目标持仓:{}, 昨日开盘价:{}, 昨日收盘价:{}, 今日开盘价：{}". \
                format(event.visible_time, current_position, net_value, dest_position, self.last_open, self.last_close,
                       current_price)
            logging.info(msg)

        self.last_open = current_price

    def market_close(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
        dest_position = 0
        current_position = 0
        net_value = None

        current_bid_ask = None
        try:
            current_bid_ask = data_portal.current_bid_ask([self.code])[self.code]
        except:
            logging.error("没有获取到最新的买卖价，code:{}".format(self.code))

        # 等待直到获取到最新的股票价格
        current_price = None
        try:
            current_price = data_portal.current_price([self.code], event.visible_time)[self.code].price
        except:
            logging.error("没有获取到当天的开盘价,code:{}".format(self.code))
        if current_price:
            net_value = account.net_value({self.code: current_price})

        if current_price and self.last_open and current_price > self.last_open:
            max_leverage = account.get_max_leverage(self.code, net_value, OrderDirection.BUY)
            leverage = min(max_leverage, self.long_leverage)
            dest_position = int(net_value * leverage / current_price)

        if len(account.positions) > 0:
            current_position = account.positions[self.code]

        change = dest_position - current_position
        if change != 0:
            direction = OrderDirection.BUY if change > 0 else OrderDirection.SELL

            reason = "当前持仓:{}, 总市值：{}, 目标持仓:{}, 昨日收盘价:{}, " \
                     "今日收盘价:{}, 当前买卖价:{}, strategy:{}".format(current_position,
                                                              net_value, dest_position,
                                                              self.last_close,
                                                              current_price,
                                                              current_bid_ask.__dict__ if current_bid_ask else None,
                                                              ACBStrategy.__doc__)
            if current_bid_ask:
                delta = 0.01
                limit_price = (current_bid_ask.bid_price + delta) if direction == OrderDirection.BUY else (
                        current_bid_ask.ask_price - delta)
                order = LimitOrder(self.code, direction, abs(change), event.visible_time, limit_price)
                order.with_reason(reason)
                account.place_order(order)
                # self.ensure_order_filled(account, data_portal, order, period=40, retry_count=1)
                self.ensure_order_filled_v2(account, data_portal, order, duration=40, delta=delta)
            else:
                order = MKTOrder(self.code, direction, abs(change), event.visible_time)
                order.with_reason(reason)
                account.place_order(order)
        else:
            logging.info("不需要下单, 时间:{}, 当前持仓:{}, 总市值：{}, 目标持仓:{}, 今日开盘价:{}, 今日收盘价:{}".
                         format(event.visible_time,
                                current_position,
                                net_value, dest_position,
                                self.last_open,
                                current_price))

        self.last_close = current_price
