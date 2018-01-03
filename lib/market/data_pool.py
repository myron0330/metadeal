# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Data pools.
# **********************************************************************************#


class MarketPoolMcs(type):

    def __new__(mcs, name, bases, attributes):
        """
        Add attributes __pool__ to object.
        """
        __pool__ = {key: value for key, value in attributes.iteritems() if key.endswith('_pool')}
        attributes['__pool__'] = __pool__
        return type.__new__(mcs, name, bases, attributes)


class MarketPool(object):
    """
    Market pool
    """
    __metaclass__ = MarketPoolMcs

    def __new__(cls, *args, **kwargs):
        """
        market pool as single instance
        """
        if not hasattr(cls, '_instance'):
            cls._instance = super(MarketPool, cls).__new__(cls)
        return cls._instance

    def test(self):
        print self.__pool__


if __name__ == '__main__':
    data = MarketPool()
    data.test()
