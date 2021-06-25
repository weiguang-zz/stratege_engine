import asyncio

from se2 import TDAccount
from se2.domain.account import *
from se2.domain.engine import *
from se2.domain.time_series import *


class NewSPCEStrategy(AbstractStrategy):
    """
    新spce策略，在开盘的时候进行判断，如果日间涨幅为正，则卖空，如果日间涨幅为负，则做多，持有时间为10分钟

    该策略不支持回测
    """

    def do_order_status_change(self, order):
        pass

    def do_initialize(self):
        market_open = EventDefinition(tp=EventDefinitionType.TIME, time_rule=MarketOpen(minute_offset=-4152))
        market_close = EventDefinition(tp=EventDefinitionType.TIME, time_rule=MarketClose(second_offset=0))
        ten_minute_after_market_open = EventDefinition(tp=EventDefinitionType.TIME,
                                                       time_rule=MarketOpen(minute_offset=10))
        self.engine.register_event(market_open, self.market_open)
        self.engine.register_event(market_close, self.market_close)
        self.engine.register_event(ten_minute_after_market_open, self.ten_minute_after_market_open)
        # 初始化收盘价
        # self.initialize_price()
        self.last_close = 10
        if isinstance(self.account, TDAccount):
            asyncio.run(self.account.streamer_account_re_sub())

    @alarm(level=AlarmLevel.ERROR, target="开盘回调", escape_params=[EscapeParam(index=0, key='self')])
    def market_open(self, event: Event):
        dest_position = 0
        current_position = 0
        net_value = None
        market_open_time = event.visible_time

        # 获取到最新的开盘价格
        current_price: CurrentPrice = None
        allow_delay = Timedelta(seconds=5)
        while True:
            if (Timestamp.now(tz='Asia/Shanghai') - market_open_time) > allow_delay:
                raise RuntimeError("没有获取到最新价格")
            current_prices = self.data_portal.current_price([self.code], None)
            if self.code in current_prices:
                if current_prices[self.code].visible_time >= market_open_time:
                    current_price = current_prices[self.code]
                    break
            time.sleep(0.5)
        # current_price = CurrentPrice("tt", Timestamp.now(tz='Asia/Shanghai'), "SPCE_STK_USD_SMART", {"price":50, 'ask_price':51, 'ask_size':10, 'bid_price':49, 'bid_size':9})
        # 计算账户净值
        net_value = self.account.net_value({self.code: current_price.price})

        if len(self.account.positions) > 0:
            current_position = self.account.positions[self.code]

        if current_price.price > self.last_close:
            dest_position = -int(net_value / current_price.price)
        elif current_price.price < self.last_close:
            dest_position = int(net_value / current_price.price)

        change = dest_position - current_position
        if change != 0:
            direction = OrderDirection.BUY if change > 0 else OrderDirection.SELL
            reason = "时间:{}, 当前持仓:{}, 总市值：{}, 目标持仓:{}, 昨日收盘价:{}, 今日最新价格:{}" \
                .format(event.visible_time, current_position, net_value, dest_position, self.last_close,
                        current_price.__dict__)
            bargainer = Bargainer(self.account, self.data_portal.current_price_ts, 5, MidPriceBargainer())
            order: Order = LimitOrder(self.code, direction, abs(change), Timestamp.now(tz='Asia/Shanghai'),
                                      reason, current_price.price, None, bargainer=bargainer)
            self.account.place_order(order)
        else:
            msg = "不需要下单, 时间:{}, 当前持仓:{}, 总市值：{}, 目标持仓:{}, 昨日收盘价:{}, 今日开盘价：{}". \
                format(event.visible_time, current_position, net_value, dest_position, self.last_close,
                       current_price.__dict__)
            logging.info(msg)

    @alarm(level=AlarmLevel.ERROR, target="平仓逻辑", escape_params=[EscapeParam(index=0, key='self')])
    def ten_minute_after_market_open(self, event: Event):
        current_position = 0
        if len(self.account.positions) > 0:
            current_position = self.account.positions[self.code]

        dest_position = 0
        change = dest_position - current_position
        if change != 0:
            direction = OrderDirection.BUY if change > 0 else OrderDirection.SELL
            current_price = self.data_portal.current_price([self.code])[self.code]
            order = MKTOrder(self.code, direction, abs(change), Timestamp.now(), "开盘十分钟后平仓", current_price.price)
            self.account.place_order(order)

    @alarm(level=AlarmLevel.ERROR, target="收盘逻辑", escape_params=[EscapeParam(index=0, key='self')])
    def market_close(self, event: Event):
        now = Timestamp.now(tz='Asia/Shanghai')
        current_prices = self.data_portal.current_price([self.code], None)
        if self.code not in current_prices:
            raise RuntimeError("没有获取到最新价格")
        if (now - current_prices[self.code].visible_time) > Timedelta(seconds=5):
            raise RuntimeError("最新价格延迟过高")
        self.last_close = current_prices[self.code].price

    def __init__(self, scope: Scope):
        super().__init__(scope)
        if isinstance(self.account, BacktestAccount):
            raise RuntimeError("not supported")
        self.last_close = None
        self.code = 'SPCE_STK_USD_SMART'

    def initialize_price(self):
        command = HistoryDataQueryCommand(None, None, [self.code], window=1)
        command.with_calendar(trading_calendar=self.scope.trading_calendar)
        df = self.data_portal.history_data("ibAdjustedDailyBar", command)
        if len(df) >= 1:
            self.last_close = df.iloc[-1]['close']
            logging.info("初始化数据成功，昨日收盘价:{}, bar的开始时间:{}"
                         .format(self.last_close, df.iloc[-1]['start_time']))
        else:
            raise RuntimeError("没有获取到昨日开盘价和收盘价")


class MidPriceBargainer(BargainAlgo):
    def get_initial_price(self, cp: CurrentPrice, bargainer: Bargainer) -> float:
        """
        初始价格有理想价格以及买一跟卖一价格来确定，如果理想价格更易成交，则以理想价格下单，如果中间价更易成交，则以中间价下单
        :param cp:
        :param bargainer:
        :return:
        """
        mid_price = (cp.bid_price + cp.ask_price) / 2
        if bargainer.order.direction == OrderDirection.BUY:
            return max(mid_price, bargainer.order.ideal_price)
            pass
        elif bargainer.order.direction == OrderDirection.SELL:
            return min(mid_price, bargainer.order.ideal_price)
            pass
        else:
            raise NotImplementedError

    def bargain(self, cp: CurrentPrice, bargainer: Bargainer) -> PriceChange:
        """
        会根据最新的价格来计算中间价，并更新我的出价为中间价，前提是市场买一跟卖一价格不是我的出价
        :param cp:
        :param bargainer:
        :return:
        """
        if not isinstance(bargainer.order, LimitOrder):
            raise RuntimeError("wrong order type")
        if bargainer.order.direction == OrderDirection.BUY:
            if round(bargainer.last_price(), 2) == round(cp.bid_price, 2):
                logging.info("当前出价是市场最优价格，不做调整")
                return None
            new_price = round((cp.bid_price + cp.ask_price) / 2, 2)
            return PriceChange(Timestamp.now(tz='Asia/Shanghai'), bargainer.last_price(), new_price, cp)
        elif bargainer.order.direction == OrderDirection.SELL:
            if round(bargainer.last_price(), 2) == round(cp.ask_price, 2):
                logging.info("当前出价是市场最优价格，不做调整")
                return None
            new_price = round((cp.bid_price + cp.ask_price) / 2, 2)
            return PriceChange(Timestamp.now(tz='Asia/Shanghai'), bargainer.last_price(), new_price, cp)
        else:
            raise NotImplementedError
