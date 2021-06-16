from trading_calendars import get_calendar

from se2.domain.engine import *
from se2.domain.account import *
from se2.domain.time_series import *
from new_interface.strategy_spce_new.st import NewSPCEStrategy
from se2.infras.td import *
from se2.infras.ib2 import *
from se2 import config

account_name = 'td_server_spce'
repo: AccountRepo = BeanContainer.getBean(AccountRepo)
acc: TDAccount = repo.find_one(account_name)
if not acc:
    acc = TDAccount(account_name, 1000, config.get('td', 'account_id'))

scope = Scope(["SPCE_STK_USD_SMART"], trading_calendar=get_calendar("NYSE"))

data_portal = DataPortal(False, "ibCurrentPrice", scope.codes)
engine = Engine(acc, data_portal)
strategy = NewSPCEStrategy(scope)

engine.run(strategy)
