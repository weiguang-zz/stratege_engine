#  注册
from se.domain2.time_series.ib import IBMinBar
from se.domain2.time_series.time_series import TSFunctionRegistry

TSFunctionRegistry.register(IBMinBar())