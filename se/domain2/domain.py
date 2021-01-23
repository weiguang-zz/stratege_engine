from typing import Mapping


class BeanContainer(object):

    beans: Mapping[type, object] = {}

    @classmethod
    def getBean(cls, the_type: type):
        return cls.beans[the_type]

    @classmethod
    def register(cls, the_type: type, bean: object):
        cls.beans[the_type] = bean
