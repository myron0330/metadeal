# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: error utils
# **********************************************************************************#
import traceback
from configs import logger


error = (lambda code, message: {'code': code, 'data': message})


def _logging_exception():
    """
    Logging exception
    """
    logger.error(traceback.format_exc())


def take_care_of_exception(func):
    """
    Decorator: Deal with exception
    """
    def _decorator(obj, *args, **kwargs):
        try:
            response = func(obj, *args, **kwargs)
        except tuple(Errors.error_types()), error_code:
            _logging_exception()
            response = error_code.args[0]
        except:
            _logging_exception()
            response = error(500, '[{}] Exceptions.'.format(func.func_name))
        return response
    return _decorator


class SchemaException(Exception):
    """
    Schema exception.
    """
    pass


class DatabaseException(Exception):
    """
    Database exception.
    """
    pass


class TradeException(Exception):
    """
    Trade exception.
    """
    pass


class Errors(object):

    INVALID_SCHEMA_ID_INPUT = SchemaException(error(500, '[SchemaException] INVALID Schema ID INPUT.'))
    INVALID_SCHEMA_TYPE = SchemaException(error(500, '[SchemaException] INVALID Schema type.'))

    INVALID_DATABASE = DatabaseException(error(500, '[DatabaseException] INVALID database.'))

    INVALID_FILLED_AMOUNT = TradeException(error(500, '[TradeException] INVALID filled amount.'))

    @classmethod
    def enumerates(cls):
        return [value for attr, value in cls.__dict__.iteritems()]

    @classmethod
    def error_types(cls):
        return tuple([Exception, ImportError, NotImplementedError,
                      SchemaException, DatabaseException, TradeException])
