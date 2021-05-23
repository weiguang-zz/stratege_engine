from trading_calendars import get_calendar

from se import config, AccountRepo, BeanContainer
from se.domain2.engine.engine import Engine, Scope
from se.infras.ib import IBAccount
from strategies.acb import ACBStrategy

engine = Engine()
scope = Scope(["ACB_STK_USD_SMART"], trading_calendar=get_calendar("NYSE"))
strategy = ACBStrategy(scope)

account_name = "ib_real_acb"
repo: AccountRepo = BeanContainer.getBean(AccountRepo)
acc = repo.find_one(account_name)
if not acc:
    acc = IBAccount(account_name, 15000)

acc.with_order_callback(strategy).with_client(config.get('ib_account', 'host'), config.getint('ib_account', 'port'),
                                              config.getint('ib_account', 'client_id'))
acc.valid_scope(scope)
acc.start_save_thread()
acc.start_sync_order_executions_thread()

engine.run(strategy, acc)