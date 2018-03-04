# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Strategy FILE
#   Author: Myron
# **********************************************************************************#
from inspect import isfunction


def blank_func(any_input):
    pass


valid_functions = [
    'initialize',
    'handle_data',
    'on_tick',
    'on_bar',
    'on_order',
    'on_trade',
    'on_order_book'
]


class TradingStrategy(object):
    """
    Trading strategy class.
    """

    def __init__(self, initialize=blank_func, handle_data=blank_func, **other_functions):
        if not hasattr(initialize, '__call__'):
            raise ValueError('Exception in "TradingStrategy": initialize must be a function!')
        else:
            self.initialize = initialize

        if handle_data and not hasattr(handle_data, '__call__'):
            raise ValueError('Exception in "TradingStrategy": handle_data must be a function!')
        else:
            self.handle_data = handle_data

        self.__dict__.update({attribute: func for attribute, func in other_functions.iteritems()
                              if self._validate(attribute, func)})

    def get_functions(self):
        """
        Get functions
        """
        return self.__dict__

    @staticmethod
    def _validate(attribute, func):
        """
        Validate the trading strategy input

        Args:
            attribute(string): function name
            func(func): function

        Returns:
            boolean: validation result
        """
        if attribute not in valid_functions or (not isfunction(func)):
            return False
        return True

    def __repr__(self):
        return "{class_name}({class_func})".format(class_name=self.__class__.__name__,
                                                   class_func=', '.join([attribute for attribute in valid_functions
                                                                         if hasattr(self, attribute)]))
