# 概述
strategy_engine是一个集回测、研究、实盘为一体的策略引擎， 其优势在于
* 可以集成任意市场、任意品种的数据进行回测和实盘
* 本地大数据存储方案，提升性能
* 回测、实盘无缝迁移
* 基于假设检验的策略有效性检验框架，尽量排除随机性

# QUICK START
## 安装

## 写策略
```python


```
## 运行
```python
## 实盘
strategy = TestStrategy1(code="CCL_STK_USD_SMART", n=5, p=0.01)
current_price_loader = TickCurrentPriceLoader(tick_loader=HistoryDataLoader(data_provider_name="ib",
                                                                            ts_type_name='tick'),
                                              calendar=strategy.trading_calendar, is_realtime=True)
account = IBAccount("192.168.0.221", 4002, 8, 30000)
engine = StrategyEngine(None)
engine.run(strategy, account, current_price_loader)
```

# 领域模型