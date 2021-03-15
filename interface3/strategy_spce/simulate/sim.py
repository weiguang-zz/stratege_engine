from trading_calendars import get_calendar

from se import config, BeanContainer, AccountRepo
from se.domain2.engine.engine import Engine, Scope
from se.infras.ib import IBAccount
from strategies.spce import SPCEStrategy

engine = Engine()
scope = Scope(["SPCE_STK_USD_SMART"], trading_calendar=get_calendar("NYSE"))
strategy = SPCEStrategy(scope)

account_name = "ib_sim1"
repo: AccountRepo = BeanContainer.getBean(AccountRepo)
acc = repo.find_one(account_name)
if not acc:
    acc = IBAccount(account_name, 10000)

acc.with_order_callback(strategy).with_client(config.get('ib_account', 'host'), config.getint('ib_account', 'port'),
                                              config.getint('ib_account', 'client_id'))
engine.run(strategy, acc)
