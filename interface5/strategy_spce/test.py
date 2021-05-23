import os
os.environ['PYTHONPATH'] = '/Users/zhangzheng/Quantitative/stratege_engine'
os.environ['config.dir'] = "/Users/zhangzheng/Quantitative/stratege_engine/interface5"


from se.infras.td import TDAccount
from se import config


acc = TDAccount("td_test", 100)
acc.with_order_callback(None).with_client(config.get("td_account", "client_id"),
                                              config.get("td_account", 'redirect_url'),
                                              config.get("td_account", 'credentials_path'),
                                              config.get("td_account", 'account_id'))
o = acc.client.search_instruments("SPCEXX", "symbol-search")
o['SPCE']

print("done")