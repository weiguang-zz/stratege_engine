from typing import List, Mapping

from cassandra.cqlengine.query import ModelQuerySet, BatchQuery
from pandas._libs.tslibs.timedeltas import Timedelta
from pandas._libs.tslibs.timestamps import Timestamp

from se.domain2.account.account import AccountRepo, AbstractAccount, Operation, CrossMKTOrder, BacktestAccount, \
    MKTOrder, OrderDirection, LimitOrder, OrderExecution, DelayMKTOrder, CrossDirection, OrderStatus
from se.domain2.time_series.time_series import TimeSeriesRepo, TimeSeries, DataRecord, TimeSeriesFunction, \
    TSFunctionRegistry, TSData
from se.infras.ib import IBAccount
from se.infras.models import TimeSeriesModel, DataRecordModel, TimeSeriesDataModel, AccountModel, OperationModel, \
    UserOrderModel, UserOrderExecutionModel


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
        else:
            raise RuntimeError("wrong account type")

        account.cash = account_model.cash
        account.initial_cash = account_model.initial_cash
        account.positions = account_model.positions
        account.history_net_value = {Timestamp(dt, tz='Asia/Shanghai'): account_model.history_net_value[dt]
                                     for dt in account_model.history_net_value}
        account.current_operation = self._to_operation(account_model.current_operation)
        account.history_operations = [self._to_operation(op) for op in account_model.history_operations]
        return account

    def save(self, account: AbstractAccount):
        tp = type(account).__name__
        acc = AccountModel.create(tp=tp, name=account.name, cash=account.cash, initial_cash=account.initial_cash,
                                  positions=account.positions,
                                  history_net_value=account.history_net_value,
                                  current_operation=self._to_operation_model(account.current_operation),
                                  history_operations=[self._to_operation_model(operation) for operation in
                                                      account.history_operations])
        acc.save()

    def _to_operation_model(self, operation: Operation):
        order_models = []
        for o in operation.orders:
            kwargs = o.__dict__
            execution_map = {exec_id: UserOrderExecutionModel(**o.execution_map[exec_id].__dict__)
                             for exec_id in o.execution_map}
            kwargs.update({"direction": o.direction.value, "status": o.status.value, "execution_map": execution_map})
            if isinstance(o, CrossMKTOrder):
                kwargs.update({"cross_direction": o.cross_direction.value if o.cross_direction else None})
            if isinstance(o, DelayMKTOrder):
                kwargs.update({"delay_time": str(o.delay_time)})
            order_models.append(
                UserOrderModel(type=type(o).__name__, **kwargs))
        return OperationModel(start_time=operation.start_time,
                              end_time=operation.end_time,
                              pnl=operation.pnl, start_cash=operation.start_cash, orders=order_models)

    def _to_operation(self, operation_model: OperationModel) -> Operation:
        op = Operation(operation_model.start_cash)
        op.start_cash = operation_model.start_cash
        op.pnl = operation_model.pnl
        op.start_time = operation_model.start_time
        op.end_time = operation_model.end_time
        for od_model in operation_model.orders:
            if isinstance(od_model, UserOrderModel):
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
                o.execution_map = {exec_id: OrderExecution(id=exec_id,
                                                           commission=od_model.execution_map[exec_id].commission,
                                                           origin=od_model.execution_map[exec_id].origin)
                                   for exec_id in od_model.execution_map.keys()}
                o.filled_start_time = self._to_timestamp(od_model.filled_start_time)
                o.filled_end_time = self._to_timestamp(od_model.filled_end_time)
                o.filled_avg_price = od_model.filled_avg_price
                o.filled_quantity = od_model.filled_quantity
                o.status = OrderStatus(od_model.status)
                op.orders.append(o)

        return op

    def _to_timestamp(self, dt) -> Timestamp:
        return Timestamp(dt, tz='Asia/Shanghai')


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
