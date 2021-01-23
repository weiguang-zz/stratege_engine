import logging
import os
import time


def SetupLogger():
    if not os.path.exists("log"):
        os.makedirs("log")

    time.strftime("application.%Y%m%d_%H%M%S.log")

    recfmt = '(%(threadName)s) %(asctime)s.%(msecs)03d %(levelname)s %(filename)s:%(lineno)d %(message)s'
    timefmt = '%y%m%d_%H:%M:%S'

    # logging.basicConfig( level=logging.DEBUG,
    #                    format=recfmt, datefmt=timefmt)
    logging.basicConfig(filename=time.strftime("log/application.%y%m%d.log"),
                        filemode="a",
                        level=logging.INFO,
                        format=recfmt, datefmt=timefmt)
    logger = logging.getLogger()
    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter(fmt=recfmt, datefmt=timefmt))
    console.setLevel(logging.DEBUG)
    logger.addHandler(console)


SetupLogger()


# # 初始化依赖注入框架
# class MyBindingSpec(BindingSpec):
#     def configure(self, bind):
#         bind("config", to_instance=config, in_scope=pinject.SINGLETON)
#
#
# obj_graph = pinject.new_object_graph(modules=None, classes=[ConfigParser],
#                                      binding_specs=[MyBindingSpec()], only_use_explicit_bindings=True)
