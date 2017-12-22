# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Data pools.
# **********************************************************************************#


class DataPoolMcs(type):

    def __new__(mcs, name, bases, attributes):
        """
        Add attributes __pool__ to object.
        """
        __pool__ = {key: value for key, value in attributes.iteritems() if key.endswith('_pool')}
        attributes['__pool__'] = __pool__
        return type.__new__(mcs, name, bases, attributes)


class DataPool(object):
    """
    Data pool in single instance.
    """
    __metaclass__ = DataPoolMcs

    def __new__(cls, *args, **kwargs):
        """
        Single instance data pool.
        """
        if not hasattr(cls, '_instance'):
            cls._instance = super(DataPool, cls).__new__(cls)
        return cls._instance

    daily_pool = None
    history_pool = None

    def test(self):
        print self.__pool__


if __name__ == '__main__':
    data = DataPool()
    data.test()
