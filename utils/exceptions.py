"""
# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Exception file.
# **********************************************************************************#
"""

error_wrapper = (lambda code, message: {'code': code, 'data': message, 'msg': message})


def deal_with_exception(func):
    """
    Deal with exception.
    """
    def _decorator(obj, *args, **kwargs):
        try:
            response = func(obj, *args, **kwargs)
        except tuple(Exceptions.error_types()) as error_code:
            response = error_code.args[0]
        except:
            response = error_wrapper(500, 'Exception unknown.'.format(func.func_name))
        return response
    return _decorator


class EnvironmentsException(Exception):
    """
    Exception in module environments.
    """
    pass


class TradeException(Exception):
    """
    Exception in trade.
    """
    pass


class BaseExceptionEnumerate(object):
    """
    Base exception enumerate.
    """
    @classmethod
    def enumerates(cls):
        """
        all exceptions enumerate.
        """
        return [value for attr, value in cls.__dict__.items()]

    @classmethod
    def error_types(cls):
        """
        all error types enumerate.
        """
        return tuple([
            EnvironmentsException
        ])


class Exceptions(BaseExceptionEnumerate):
    """
    Enumerate exceptions.
    """
    INVALID_INITIALIZE_PARAMETERS = EnvironmentsException(error_wrapper(500, 'You have invalid input parameters'
                                                                             ' when you initialize your environment.'))


class ExceptionsFormat(BaseExceptionEnumerate):
    """
    Enumerate exceptions format.
    """
    NOT_IN_ACTION_SPACE = EnvironmentsException(error_wrapper(500, 'Your action {} is not in action space {}.'))
    INVALID_SECURITY_TYPE = TradeException(error_wrapper(500, 'Invalid security type: {}.'))
    INVALID_FILLED_AMOUNT = TradeException(error_wrapper(500, 'Invalid filled amount: {}.'))


__all__ = [
    'Exceptions',
    'ExceptionsFormat'
]
