import logging
import threading
import time

from pandas._libs.tslibs.timestamps import Timestamp

from se.domain2.account.account import AbstractAccount, MKTOrder, OrderDirection, LimitOrder, OrderStatus
from se.domain2.domain import send_email
from se.domain2.engine.engine import AbstractStrategy, Engine, EventDefinition, EventDefinitionType, MarketOpen, \
    MarketClose, Event, DataPortal
import numpy as np
from se.domain2.time_series.time_series import HistoryDataQueryCommand


class TestStrategy2(AbstractStrategy):
    """
    该策略会在收盘的时候，检查今天收盘是否大于昨日收盘，如果大于，则以市价买入，并持有到下一个开盘卖出
    会在开盘的时候判断，如果昨天日内上涨，则在开盘的时候进行卖空，并且在收盘的时候平仓
    交易标的： SPCE
    """

    def do_initialize(self, engine: Engine, data_portal: DataPortal):
        if engine.is_backtest:
            market_close = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketClose())
            market_open = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketOpen())
        else:
            market_open = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketOpen(second_offset=5))
            market_close = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketClose(second_offset=-60))
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
            # self.last_open = 38.11
            # self.last_close = 38.14

        if len(self.scope.codes) != 1:
            raise RuntimeError("wrong codes")
        self.code = self.scope.codes[0]

    def ensure_order_filled(self, account: AbstractAccount, data_portal: DataPortal, order: LimitOrder, period: int, retry_count: int):
        """
        将通过如下算法来保证订单一定成交：
        每隔一定时间检查订单是否还未成交，如果是的话，则取消原来的订单，并且按照最新的价格下一个新的订单，同时监听新的订单
        :param account:
        :param order:
        :param period:
        :param retry_count:
        :return:
        """
        def ensure():
            try:
                time.sleep(period)
                if order.status == OrderStatus.CREATED or order.status == OrderStatus.PARTIAL_FILLED:
                    account.cancel_open_order(order)
                    remain_quantity = order.quantity - order.filled_quantity
                    cp = data_portal.current_price([self.code], None)[self.code]
                    new_quantity = int(remain_quantity * order.limit_price / cp.price)
                    now = Timestamp.now(tz='Asia/Shanghai')
                    if retry_count > 1:
                        new_order = LimitOrder(order.code, order.direction, new_quantity,
                                               now, cp.price)
                    else:
                        new_order = MKTOrder(order.code, order.direction, new_quantity, now)
                    msg = "由于订单没有成交，取消了原来的订单，并且重新下单， 原来的订单:{}, 新的订单:{}".\
                        format(order.__dict__, new_order.__dict__)
                    logging.info(msg)
                    send_email('[订单]', msg)
                    account.place_order(new_order)
                    if retry_count > 1:
                        self.ensure_order_filled(account, data_portal, new_order, period, retry_count-1)
                else:
                    logging.info("没有还未成交的订单, 不需要ensure")
            except:
                import traceback
                err_msg = "ensure失败:{}".format(traceback.format_exc())
                logging.error(err_msg)
                send_email("[订单]ensure失败", err_msg)

        if retry_count <= 0:
            raise RuntimeError('wrong retry count')
        if not isinstance(order, LimitOrder):
            raise RuntimeError("wrong order type")

        threading.Thread(name='ensure_order_filled', target=ensure).start()

    def market_open(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
        dest_position = 0
        current_position = 0
        net_value = None

        # 等待直到获取到最新的股票价格
        current_price = self.get_recent_price_after([self.code], event.visible_time, data_portal)
        if not current_price:
            msg = "没有获取到当天的开盘价,code:{}".format(self.code)
            logging.error(msg)
            send_email("【系统】没有获取到当前价格", msg)
        else:
            net_value = account.net_value({self.code: current_price})

        if len(account.positions) > 0:
            current_position = account.positions[self.code]

        if current_price and self.last_open and self.last_close and self.last_close > self.last_open:
            dest_position = - int(net_value / current_price)

        change = dest_position - current_position
        if change != 0:
            direction = OrderDirection.BUY if change > 0 else OrderDirection.SELL
            if current_price:
                order = LimitOrder(self.code, direction, abs(change), event.visible_time, current_price)
                account.place_order(order)
                self.ensure_order_filled(account, data_portal, order, 60, 3)
            else:
                order = MKTOrder(self.code, direction, abs(change), event.visible_time)
                account.place_order(order)
            msg = "时间:{}, 当前持仓:{}, 总市值：{}, 目标持仓:{}, 昨日开盘价:{}, 昨日收盘价:{}, 今日开盘价：{}, 订单:{}" \
                .format(event.visible_time, current_position, net_value, dest_position, self.last_open, self.last_close,
                        current_price, order.__dict__)
            logging.info("开盘下单:{}".format(msg))
            send_email('【订单】下单', msg)
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

        current_price = self.get_recent_price_after([self.code], event.visible_time, data_portal)
        if not current_price:
            msg = "没有获取到当前价格, code:{}".format(self.code)
            logging.error(msg)
            send_email("【系统】没有获取到当前价格", msg)
        else:
            net_value = account.net_value({self.code: current_price})

        if current_price and self.last_close and current_price > self.last_close:
            dest_position = int(net_value / current_price)

        if len(account.positions) > 0:
            current_position = account.positions[self.code]

        change = dest_position - current_position
        if change != 0:
            direction = OrderDirection.BUY if change > 0 else OrderDirection.SELL

            if current_price:
                order = LimitOrder(self.code, direction, abs(change), event.visible_time, current_price)
                account.place_order(order)
                self.ensure_order_filled(account, data_portal, order, 50, 1)
            else:
                order = MKTOrder(self.code, direction, abs(change), event.visible_time)
                account.place_order(order)
            msg = "时间:{}, 当前持仓:{}, 总市值：{}, 目标持仓:{}, 昨日收盘价:{}, 今日收盘价:{}, 订单:{}".format(event.visible_time,
                                                                                      current_position,
                                                                                      net_value, dest_position,
                                                                                      self.last_close,
                                                                                      current_price, order.__dict__)
            logging.info("收盘下单:{}".format(msg))
            send_email("【订单】下单", msg)
        else:
            logging.info("不需要下单, 时间:{}, 当前持仓:{}, 总市值：{}, 目标持仓:{}, 今日开盘价:{}, 今日收盘价:{}".
                         format(event.visible_time,
                                current_position,
                                net_value, dest_position,
                                self.last_open,
                                current_price))

        self.last_close = current_price

    def order_status_change(self, order, account):
        logging.info("订单状态变更, 订单状态:{}，账户持仓:{}, 账户现金:{}".
                     format(order.__dict__, account.positions, account.cash))
        msg = {"positions": account.positions, "cash": account.cash, "order": order.__dict__}
        title = "【订单】状态变更({})".format(order.status.value)
        send_email(title, str(msg))


class TestStrategy3(AbstractStrategy):
    """
    在每天收盘的时候， 如果有持仓，则平仓，并且以收盘价卖空
    在每天开盘的时候，如果有持仓(一定是空仓)，则平仓， 并且判断昨天收盘到今天开盘的涨幅是否小于0.025， 若是，则以开盘价买入
    交易标的： GSX
    """

    def do_initialize(self, engine: Engine, data_portal: DataPortal):
        if engine.is_backtest:
            market_close = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketClose())
            market_open = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketOpen())
        else:
            # 实盘的时候，在收盘前5s的时候提交订单
            market_open = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketOpen(second_offset=5))
            market_close = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketClose(second_offset=-30))
        engine.register_event(market_open, self.market_open)
        engine.register_event(market_close, self.market_close)
        self.is_backtest = engine.is_backtest
        self.code = self.scope.codes[0]
        # 初始化昨日收盘价
        self.last_close_price = None
        if not engine.is_backtest:
            command = HistoryDataQueryCommand(None, None, self.scope.codes, window=1)
            command.with_calendar(trading_calendar=self.scope.trading_calendar)
            df = data_portal.history_data("ibAdjustedDailyBar", command)
            if len(df) >= 1:
                self.last_close_price = df.iloc[-1]['close']
                logging.info("初始化数据成功，昨日收盘价:{}, bar的开始时间为:{}".
                             format(self.last_close_price, df.iloc[-1]['start_time']))
            else:
                raise RuntimeError("没有获取到昨日开盘价和收盘价")
            # self.last_close_price = 102.85

    def market_open(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
        dest_position = 0
        current_position = 0
        net_value = None

        current_price = self.get_recent_price_after([self.code], event.visible_time, data_portal)

        if not current_price:
            msg = "没有获取到当天的开盘价{}".format(self.code)
            logging.error(msg)
            send_email("【系统】没有获取到当前价格", msg)
        else:
            net_value = account.net_value({self.code: current_price})

        if self.last_close_price and current_price:
            if np.log(current_price / self.last_close_price) < 0.025:
                dest_position = int(net_value / current_price)

        if len(account.positions) > 0:
            current_position = account.positions[self.code]

        change = dest_position - current_position

        if change != 0:
            direction = OrderDirection.BUY if change > 0 else OrderDirection.SELL
            # if current_price:
            #     order = LimitOrder(self.code, direction, abs(change), event.visible_time, current_price)
            # else:
            #     logging.warning("没有获取到当前价格，将会以市价单下单")
            order = MKTOrder(self.code, direction, abs(change), event.visible_time)
            account.place_order(order)
            msg = "时间:{}, 当前持仓:{}, 总市值：{}, 目标持仓:{}, 昨日收盘价:{}, 今日开盘价：{}, 订单:{}" \
                .format(event.visible_time, current_position, net_value, dest_position, self.last_close_price,
                        current_price, order.__dict__)
            logging.info("开盘下单:{}".format(msg))
            send_email('【订单】下单', msg)
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
            send_email('【系统】没有获取到当前价格', msg)
        else:
            net_value = account.net_value({self.code: current_price})
            dest_position = -int(net_value / current_price)

        if len(account.positions) > 0:
            current_position = account.positions[self.code]
        change = dest_position - current_position

        if change != 0:
            direction = OrderDirection.BUY if change > 0 else OrderDirection.SELL
            # if current_price:
            #     order = LimitOrder(self.code, direction, abs(change), event.visible_time, current_price)
            # else:
            #     logging.warning("没有获取到当前价格，将会以市价单下单")
            order = MKTOrder(self.code, direction, abs(change), event.visible_time)
            account.place_order(order)
            msg = "时间:{}, 当前持仓:{}, 总市值：{}, 目标持仓:{}, 当前价格, 订单:{}" \
                .format(event.visible_time, current_position, net_value, dest_position,
                        current_price, order.__dict__)
            logging.info("收盘下单:{}".format(msg))
            send_email('【订单】下单', msg)
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
        title = "【订单】状态变更({})".format(order.status.value)
        send_email(title, str(msg))
