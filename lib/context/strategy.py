# -*- coding: utf-8 -*-

"""
strategy.py

trading strategy

@author: yudi.wu
"""


def blank_func(any_input):
    pass


class TradingStrategy(object):
    """
    交易策略，包含如下属性

    * self.initialize：交易策略-虚拟账户初始函数
    * self.handle_data：交易策略-每日交易指令判断函数
    * self.post_trading_day：交易策略-每日盘后用户自定义操作函数
    """

    def __init__(self, initialize=blank_func, handle_data=blank_func, post_trading_day=blank_func):
        if not hasattr(initialize, '__call__'):
            raise ValueError('Exception in "TradingStrategy": initialize must be a function!')
        else:
            self.initialize = initialize

        if handle_data and not hasattr(handle_data, '__call__'):
            raise ValueError('Exception in "TradingStrategy": handle_data must be a function!')
        else:
            self.handle_data = handle_data

        if post_trading_day and not hasattr(post_trading_day, '__call__'):
            raise ValueError('Exception in "TradingStrategy": post_trading_day must be a function!')
        else:
            self.post_trading_day = post_trading_day

    def __repr__(self):
        return "{class_name}(initialize, handle_data, post_trading_day)".format(
            class_name=self.__class__.__name__,
            init=self.initialize,
            handle_data=self.handle_data,
            post_trading_day=self.post_trading_day
        )
