# import logging
# import time
# from configparser import ConfigParser
# from typing import List, Mapping
#
# from pandas._libs.tslibs.timestamps import Timestamp
# from trading_calendars import get_calendar
# import numpy as np
#
# from se.domain2.account.account import AbstractAccount, MKTOrder, OrderDirection, LimitOrder
# from se.domain2.domain import send_email
# from se.domain2.engine.engine import AbstractStrategy, Engine, Scope, EventDefinition, EventDefinitionType, MarketOpen,\
#     MarketClose, Event, DataPortal
# from se.infras.ib import IBAccount
#
#
# class TestStrategy2(AbstractStrategy):
#     """
#     该策略会在收盘的时候，检查当天的涨跌幅，如果是阳柱，则以市价买入，并持有到下一个开盘卖出
#     交易标的： SPCE
#     """
#
#     def do_initialize(self, engine: Engine):
#         if engine.is_backtest:
#             market_close = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketClose())
#             market_open = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketOpen())
#         else:
#             market_open = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketOpen(second_offset=5))
#             market_close = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketClose(second_offset=-30))
#         engine.register_event(market_open, self.market_open)
#         engine.register_event(market_close, self.market_close)
#         self.open_price = None
#
#     def market_open(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
#         if len(account.positions) > 0:
#             for code in account.positions.keys():
#                 order = MKTOrder(code, direction=OrderDirection.SELL, quantity=account.positions[code],
#                                  place_time=event.visible_time)
#                 account.place_order(order)
#                 logging.info("开盘平仓, 订单:{}".format(order.__dict__))
#                 msg = {"positions": account.positions, "cash": account.cash, "order": order.__dict__}
#                 send_email("【订单】开盘平仓", str(msg))
#
#         # 等待直到获取到最新的股票价格
#         try:
#             self.open_price = self.get_recent_price_after(self.scope.codes, event.visible_time, data_portal)
#         except:
#             import traceback
#             msg = "没有获取到当天的开盘价{}".format(traceback.format_exc())
#             logging.error(msg)
#             send_email("ERROR", msg)
#
#
#     def market_close(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
#         if not len(self.scope.codes) == 1:
#             raise RuntimeError("wrong scope")
#         if len(account.positions) > 0:
#             raise RuntimeError("wrong positions")
#         if not self.open_price:
#             logging.warning("没有设置开盘价, 不操作")
#             return
#         code = self.scope.codes[0]
#         cp = data_portal.current_price([code], event.visible_time)
#         if cp[code].price > self.open_price:
#             buy_quantity = int(account.cash / cp[code].price)
#             order = LimitOrder(code, direction=OrderDirection.BUY, quantity=buy_quantity, place_time=event.visible_time,
#                                limit_price=cp[code].price)
#             account.place_order(order)
#             logging.info("当天价格上升，下单买入, 开盘价：{}, 收盘价:{}, 订单：{}".
#                          format(self.open_price, cp[code].price, order.__dict__))
#             msg = {"positions": account.positions, "cash": account.cash, "order": order.__dict__}
#             send_email("【订单】收盘买入", str(msg))
#         else:
#             logging.info("当天价格下跌，不开仓 开盘价:{}, 收盘价:{}".format(self.open_price, cp[code].price))
#
#         # 清理开盘价
#         self.open_price = None
#
#     def order_status_change(self, order, account):
#         logging.info("订单状态变更, 订单状态:{}，账户持仓:{}, 账户现金:{}".
#                      format(order.__dict__, account.positions, account.cash))
#         msg = {"positions": account.positions, "cash": account.cash, "order": order.__dict__}
#         send_email("【订单】成交", str(msg))
#
#
#
# class TestStrategy3(AbstractStrategy):
#     """
#     在每天收盘的时候， 如果有持仓，则平仓，并且以收盘价卖空
#     在每天开盘的时候，如果有持仓(一定是空仓)，则平仓， 并且判断昨天收盘到今天开盘的涨幅是否小于0.025， 若是，则已开盘价买入
#     交易标的： GSX
#     """
#
#     def do_initialize(self, engine: Engine):
#         if engine.is_backtest:
#             market_close = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketClose())
#             market_open = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketOpen())
#         else:
#             # 实盘的时候，在收盘前5s的时候提交订单
#             market_open = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketOpen(second_offset=5))
#             market_close = EventDefinition(ed_type=EventDefinitionType.TIME, time_rule=MarketClose(second_offset=-30))
#         engine.register_event(market_open, self.market_open)
#         engine.register_event(market_close, self.market_close)
#         self.open_price = None
#         self.last_close_price = None
#         self.is_backtest = engine.is_backtest
#
#     def market_open(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
#         self.open_price = None
#         code = self.scope.codes[0]
#         buy_amount = 0
#         if self.last_close_price:
#             # 获取开盘价格
#             try:
#                 self.open_price = self.get_recent_price_after([code], event.visible_time, data_portal)
#             except:
#                 import traceback
#                 msg = "没有获取到当天的开盘价{}".format(traceback.format_exc())
#                 logging.error(msg)
#                 send_email("ERROR", msg)
#             if self.open_price:
#                 if ((self.open_price - self.last_close_price) / self.last_close_price) <= 0.025:
#                     net_value = account.net_value({code: self.open_price})
#                     buy_amount += int(net_value / self.open_price)
#
#         if len(account.positions) > 0:
#             if len(account.positions) > 1:
#                 raise RuntimeError("非法的持仓")
#             buy_amount += -account.positions[code]
#         if buy_amount > 0:
#             order = MKTOrder(code, direction=OrderDirection.BUY, quantity=buy_amount,
#                              place_time=event.visible_time)
#             account.place_order(order)
#             msg = "开盘买入, 昨收:{}, 开盘价:{}, 当前持仓:{}， 订单:{}".\
#                 format(self.last_close_price, self.open_price, account.positions, order.__dict__)
#             logging.info(msg)
#             send_email("【订单】开盘下单", msg)
#
#     def market_close(self, event: Event, account: AbstractAccount, data_portal: DataPortal):
#         self.last_close_price = None
#         code = self.scope.codes[0]
#         sell_amount = 0
#         current_price = data_portal.current_price([code], event.visible_time)[code].price
#         self.last_close_price = current_price
#         if len(account.positions) > 0:
#             if len(account.positions) > 1:
#                 raise RuntimeError("非法的持仓")
#             sell_amount += account.positions[code]
#
#         net_value = account.net_value({code: current_price})
#         sell_amount += int(net_value / current_price)
#
#         if sell_amount > 0:
#             order = LimitOrder(code, direction=OrderDirection.SELL, quantity=sell_amount,
#                                place_time=event.visible_time,
#                                limit_price=current_price)
#
#             account.place_order(order)
#             msg = "收盘卖空, 当前价格:{}, 当前持仓:{}, 订单:{}".format(current_price, account.positions, order.__dict__)
#             logging.info(msg)
#             send_email("【订单】收盘卖空", msg)
#
#     def order_status_change(self, order, account):
#         logging.info("订单状态变更, 订单状态:{}，账户持仓:{}, 账户现金:{}".
#                      format(order.__dict__, account.positions, account.cash))
#         msg = {"positions": account.positions, "cash": account.cash, "order": order.__dict__}
#         send_email("【订单】成交", str(msg))