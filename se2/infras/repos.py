from cassandra.cqlengine.query import ModelQuerySet, BatchQuery

from se2.domain.account import AccountRepo, AbstractAccount, OrderRepo, Order, LimitOrder, Execution, PriceChange, \
    OrderDirection, MKTOrder, StopOrder
from se2.domain.engine import BacktestAccount
from se2.domain.time_series import *
from se2.infras.ib import IBAccount
from se2.infras.models import *
from se2.infras.td import TDAccount


def to_timestamp(dt) -> Timestamp:
    if dt:
        return Timestamp(dt, tz='UTC').tz_convert('Asia/Shanghai')
    else:
        return None


class TimeSeriesRepoImpl(TimeSeriesRepo):
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
        tp: TimeSeriesType = TSTypeRegistry.find_function(name)
        if not tp:
            raise RuntimeError("没有找到实例方法")
        ts.with_type(tp)
        return ts


class TSDataRepoImpl(TSDataRepo):
    def query(self, ts_name, command):
        data_list: List[TSData] = []
        if command.start and command.end:
            r: ModelQuerySet = TimeSeriesDataModel.objects.filter(type=ts_name, code__in=command.codes,
                                                                  visible_time__gte=command.start,
                                                                  visible_time__lte=command.end).limit(None)
        else:
            r: ModelQuerySet = TimeSeriesDataModel.objects.filter(type=ts_name, code__in=command.codes,
                                                                  visible_time__lte=command.end) \
                .order_by("-visible_time").limit(command.window)

        func = TSTypeRegistry.find_function(ts_name)

        for row in r.all():
            values: Mapping[str, object] = func.deserialized(row.data)
            visible_time = Timestamp(row.visible_time, tz='UTC').tz_convert("Asia/Shanghai")
            ts_data = TSData(row.type, visible_time, row.code, values)
            data_list.append(ts_data)
        return data_list

    def save(self, ts_list: List[TSData]):
        b = BatchQuery()
        for ts_data in ts_list:
            func = TSTypeRegistry.find_function(ts_data.ts_type_name)
            value_serialized: str = func.serialize(ts_data.values)
            TimeSeriesDataModel.batch(b).create(type=ts_data.ts_type_name, code=ts_data.code,
                                                visible_time=ts_data.visible_time, data=value_serialized)
        b.execute()


class AccountRepoImpl(AccountRepo):
    def save(self, account: AbstractAccount):
        tp = type(account).__name__
        acc = AccountModel.create(tp=tp, name=account.name, cash=account.cash, initial_cash=account.initial_cash,
                                  positions=account.positions,
                                  history_net_value=account.history_net_value)
        acc.save()

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
        account.history_net_value = {to_timestamp(dt): account_model.history_net_value[dt]
                                     for dt in account_model.history_net_value}
        logging.info("账户加载成功, 当前持仓:{}, 现金:{}".format(account.positions, account.cash))
        return account


class OrderRepoImpl(OrderRepo):
    def find_by_account_name(self, account_name) -> List[Order]:
        r: ModelQuerySet = UserOrderModel.objects(account_name=account_name)
        ret = []
        if r.count() <= 0:
            return ret
        else:
            for row in r.all():
                if row.type == 'LimitOrder':
                    o = LimitOrder(row.code, OrderDirection(row.direction), row.quantity, to_timestamp(row.place_time),
                                   row.reason, row.limit_price)

                elif row.type == "MKTOrder":
                    o = MKTOrder(row.code, OrderDirection(row.direction), row.quantity, to_timestamp(row.place_time),
                                 row.reason)
                elif row.type == "StopOrder":
                    o = StopOrder(row.code, OrderDirection(row.direction), row.quantity, to_timestamp(row.place_time),
                                  row.reason, row.stop_price)
                else:
                    raise RuntimeError("非法的订单类型")
                # todo，补充订单模型的其他属性
                ret.append(o)
        return ret

    def save(self, order: Order):
        tp = type(order).__name__
        kwargs = order.__dict__.copy()
        execution_model_map = {id: self._to_execution_model(order.executions[id]) for id in order.executions}
        kwargs.update({"direction": order.direction.value, "status": order.status.value,
                       "execution_map": execution_model_map, "type": tp})
        if isinstance(order, LimitOrder):
            price_change_models = [self._to_price_change_model(price_change)
                                   for price_change in order.price_change_history]
            kwargs.update({"price_change_history": price_change_models})
        for name in ['extended_time', 'order_status_callback', 'executions']:
            kwargs.pop(name)
        order_model = UserOrderModel.create(**kwargs)
        order_model.save()

    def _to_execution_model(self, execution: Execution) -> UserOrderExecutionModel:
        return UserOrderExecutionModel(**execution.__dict__)

    def _to_price_change_model(self, price_change: PriceChange):
        kwargs = price_change.__dict__.copy()
        kwargs.update({"current_price": CurrentPriceModel(**price_change.current_price.__dict__)})
        return PriceChangeModel(**kwargs)
