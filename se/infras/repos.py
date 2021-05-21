from typing import List, Mapping

from cassandra.cqlengine.query import ModelQuerySet, BatchQuery
from pandas._libs.tslibs.timedeltas import Timedelta
from pandas._libs.tslibs.timestamps import Timestamp

from se.domain2.account.account import AccountRepo, AbstractAccount, Operation, CrossMKTOrder, BacktestAccount, \
    MKTOrder, OrderDirection, LimitOrder, OrderExecution, DelayMKTOrder, CrossDirection, OrderStatus, Order
from se.domain2.time_series.time_series import TimeSeriesRepo, TimeSeries, DataRecord, TimeSeriesFunction, \
    TSFunctionRegistry, TSData
from se.infras.ib import IBAccount
from se.infras.models import TimeSeriesModel, DataRecordModel, TimeSeriesDataModel, AccountModel, \
    UserOrderModel, UserOrderExecutionModel
import logging

from se.infras.td import TDAccount


class AccountRepoImpl(AccountRepo):

    def find_one(self, account_name):
        accounts = AccountModel.objects(name=account_name).all()
        if not accounts or len(accounts) <= 0:
            return None
        account_model: AccountModel = accounts[0]
        if account_model.tp == 'IBAccount':
            account = IBAccount(account_model.name, account_model.initial_cash)
        elif account_model.tp == 'BacktestAccount':
            account = BacktestAccount(account_model.name, account_model.initial_cash)
        elif account_model.tp == "TDAccount":
            account = TDAccount(account_model.name, account_model.initial_cash)
        else:
            raise RuntimeError("wrong account type")

        account.cash = account_model.cash
        account.initial_cash = account_model.initial_cash
        account.positions = account_model.positions
        account.history_net_value = {self._to_timestamp(dt): account_model.history_net_value[dt]
                                     for dt in account_model.history_net_value}
        account.orders = [self._to_order(om) for om in account_model.orders]
        # account.current_operation = self._to_operation(account_model.current_operation)
        # account.history_operations = [self._to_operation(op) for op in account_model.history_operations]
        logging.info("账户加载成功, 当前持仓:{}, 现金:{}".format(account.positions, account.cash))
        return account

    def save(self, account: AbstractAccount):
        tp = type(account).__name__
        acc = AccountModel.create(tp=tp, name=account.name, cash=account.cash, initial_cash=account.initial_cash,
                                  positions=account.positions,
                                  history_net_value=account.history_net_value,
                                  orders=[self._to_order_model(o) for o in account.orders],
                                  # current_operation=self._to_operation_model(account.current_operation),
                                  # history_operations=[self._to_operation_model(operation) for operation in
                                  #                     account.history_operations]
                                  )
        acc.save()

    def _to_timestamp(self, dt) -> Timestamp:
        if dt:
            return Timestamp(dt, tz='UTC').tz_convert('Asia/Shanghai')
        else:
            return None

    def _to_order(self, od_model: UserOrderModel) -> Order:
        if od_model.type == "MKTOrder":
            o = MKTOrder(od_model.code, OrderDirection(od_model.direction), od_model.quantity,
                         self._to_timestamp(od_model.place_time))
        elif od_model.type == "LimitOrder":
            o = LimitOrder(od_model.code, OrderDirection(od_model.direction), od_model.quantity,
                           self._to_timestamp(od_model.place_time), od_model.limit_price)
        elif od_model.type == "DelayMKTOrder":
            o = DelayMKTOrder(od_model.code, OrderDirection(od_model.direction), od_model.quantity,
                              self._to_timestamp(od_model.place_time),
                              Timedelta(od_model.delay_time))
        elif od_model.type == 'CrossMKTOrder':
            o = CrossMKTOrder(od_model.code, OrderDirection(od_model.direction), od_model.quantity,
                              self._to_timestamp(od_model.place_time), CrossDirection(od_model.cross_direction),
                              od_model.cross_price)
        else:
            raise RuntimeError("wrong order type")
        new_execution_map = {}
        for exec_id in od_model.execution_map.keys():
            oe_model: UserOrderExecutionModel = od_model.execution_map[exec_id]
            oe = OrderExecution(exec_id, oe_model.version, oe_model.commission, oe_model.filled_quantity,
                                oe_model.filled_avg_price, self._to_timestamp(oe_model.filled_start_time),
                                self._to_timestamp(oe_model.filled_end_time), OrderDirection(oe_model.direction),
                                oe_model.attributes)
            new_execution_map[exec_id] = oe
        o.execution_map = new_execution_map

        o.filled_start_time = self._to_timestamp(od_model.filled_start_time)
        o.filled_end_time = self._to_timestamp(od_model.filled_end_time)
        o.filled_avg_price = od_model.filled_avg_price
        o.filled_quantity = od_model.filled_quantity
        o.status = OrderStatus(od_model.status)
        return o


    def _to_order_model(self, order: Order) -> UserOrderModel:
        execution_model_map = {}
        for exec_id in order.execution_map.keys():
            execution: OrderExecution = order.execution_map[exec_id]
            exec_model = UserOrderExecutionModel(id=execution.id, version=execution.version,
                                                 commission=execution.commission, direction=execution.direction.value,
                                                 filled_quantity=execution.filled_quantity,
                                                 filled_avg_price=execution.filled_avg_price,
                                                 filled_start_time=execution.filled_start_time,
                                                 filled_end_time=execution.filled_end_time,
                                                 attributes=execution.attributes)
            execution_model_map[exec_id] = exec_model

        kwargs = order.__dict__.copy()
        kwargs.update({"direction": order.direction.value, "status": order.status.value,
                       "execution_map": execution_model_map})
        if isinstance(order, CrossMKTOrder):
            kwargs.update({"cross_direction": order.cross_direction.value if order.cross_direction else None})
        if isinstance(order, DelayMKTOrder):
            kwargs.update({"delay_time": str(order.delay_time)})
        return UserOrderModel(type=type(order).__name__, **kwargs)



class TimeSeriesRepoImpl(TimeSeriesRepo):

    def query_ts_data(self, ts_type_name, command):
        data_list: List[TSData] = []
        if command.start and command.end:
            r: ModelQuerySet = TimeSeriesDataModel.objects.filter(type=ts_type_name, code__in=command.codes,
                                                                  visible_time__gte=command.start,
                                                                  visible_time__lte=command.end).limit(None)
        else:
            r: ModelQuerySet = TimeSeriesDataModel.objects.filter(type=ts_type_name, code__in=command.codes,
                                                                  visible_time__lte=command.end) \
                .order_by("-visible_time").limit(command.window)

        func = TSFunctionRegistry.find_function(ts_type_name)

        for row in r.all():
            values: Mapping[str, object] = func.deserialized(row.data)
            visible_time = Timestamp(row.visible_time, tz='UTC').tz_convert("Asia/Shanghai")
            ts_data = TSData(row.type, visible_time, row.code, values)
            data_list.append(ts_data)
        return data_list

    def save_ts(self, ts_list: List[TSData]):
        b = BatchQuery()
        for ts_data in ts_list:
            func = TSFunctionRegistry.find_function(ts_data.ts_type_name)
            value_serialized: str = func.serialize(ts_data.values)
            TimeSeriesDataModel.batch(b).create(type=ts_data.ts_type_name, code=ts_data.code,
                                                visible_time=ts_data.visible_time, data=value_serialized)
        b.execute()

    def save(self, ts: TimeSeries):
        data_record_map = {}
        for code in ts.data_record.keys():
            dr: DataRecord = ts.data_record[code]
            data_record_map[code] = DataRecordModel(code=dr.code, start_time=dr.start, end_time=dr.end)
        TimeSeriesModel.create(name=ts.name, data_record=data_record_map).save()

    def find_one(self, name):
        ts = TimeSeries()
        r: ModelQuerySet = TimeSeriesModel.objects(name=name)
        if r.count() == 1:
            model: TimeSeriesModel = r.first()
            data_record = {}
            for key in model.data_record.keys():
                dr_model: DataRecordModel = model.data_record[key]
                data_record[key] = DataRecord(dr_model.code, Timestamp(dr_model.start_time, tz='UTC'),
                                              Timestamp(dr_model.end_time, tz='UTC'))
            ts = TimeSeries(name=model.name, data_record=data_record)
        elif r.count() > 1:
            raise RuntimeError("wrong data")

        # 查找该实例的方法
        func: TimeSeriesFunction = TSFunctionRegistry.find_function(name)
        if not func:
            raise RuntimeError("没有找到实例方法")
        ts.with_func(func)
        return ts
