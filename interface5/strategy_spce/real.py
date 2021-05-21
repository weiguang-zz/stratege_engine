from trading_calendars import get_calendar

from se import config, BeanContainer, AccountRepo
from se.domain2.engine.engine import Engine, Scope
from se.infras.td import TDAccount
from strategies_td.spce import SPCEStrategy

engine = Engine()
scope = Scope(["SPCE_STK_USD_SMART"], trading_calendar=get_calendar("NYSE"))
strategy = SPCEStrategy(scope)

account_name = "td_real_spce"
repo: AccountRepo = BeanContainer.getBean(AccountRepo)
acc: TDAccount = repo.find_one(account_name)
if not acc:
    acc = TDAccount(account_name, 100)

acc.with_order_callback(strategy).with_client(config.get("td_account", "client_id"),
                                              config.get("td_account", 'redirect_url'),
                                              config.get("td_account", 'credentials_path'),
                                              config.get("td_account", 'account_id'))

acc.start_save_thread()

engine.run(strategy, acc)
