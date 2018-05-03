# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: error utils
# **********************************************************************************#
import logging
import traceback

error = (lambda code, message: {'code': code, 'database': message})


def _logging_exception():
    """
    Logging exception
    """
    logging.error(traceback.format_exc())


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


class TradingException(Exception):
    """
    Trading exception.
    """
    pass


class AccountException(Exception):
    """
    Account exception.
    """
    pass


class TradingInputException(Exception):
    """
    Trade input exception.
    """
    pass


class DataLoadException(Exception):
    """
    Data load exception.
    """
    pass


class HandleDataException(Exception):
    """
    Handle data exception.
    """
    pass


class InternalCheckException(Exception):
    """
    Internal check exception.
    """
    pass


class UniverseException(Exception):
    """
    Universe exception.
    """
    pass


class ContextException(Exception):
    """
    Context exception.
    """
    pass


class HistoryException(Exception):
    """
    History exception.
    """
    pass


class StrategyException(Exception):
    """
    Strategy exception.
    """
    pass


class TradingItemException(Exception):
    """
    Trade item exception.
    """
    pass


class LogException(Exception):
    """
    Log exception.
    """
    pass


class Errors(object):

    INVALID_SCHEMA_ID_INPUT = SchemaException(error(500, '[SchemaException] INVALID Schema ID INPUT.'))
    INVALID_SCHEMA_TYPE = SchemaException(error(500, '[SchemaException] INVALID Schema type.'))

    INVALID_DATABASE = DatabaseException(error(500, '[DatabaseException] INVALID database.'))

    INVALID_FILLED_AMOUNT = TradingException(error(500, '[TradingException] INVALID filled amount.'))

    INVALID_DATE = TradingInputException(error(500, '[TradingInputException] INVALID date.'))
    INVALID_FREQ = TradingInputException(error(500, '[TradingInputException] INVALID freq.'))
    INVALID_REFRESH_RATE = TradingInputException(error(500, '[TradingInputException] INVALID refresh_rate.'))
    INVALID_BENCHMARK = TradingInputException(error(500, '[TradingInputException] INVALID benchmark.'))
    INVALID_POSITION_BASE = TradingInputException(error(500, '[TradingInputException] INVALID position base.'))
    INVALID_COST_BASE = TradingInputException(error(500, '[TradingInputException] INVALID cost base.'))
    INVALID_CAPITAL_BASE = TradingInputException(error(500, '[TradingInputException] INVALID capital base.'))
    INVALID_SECURITY_ID = TradingInputException(error(500, '[TradingInputException] INVALID symbol.'))

    INVALID_UNIVERSE = UniverseException(error(500, '[UniverseException] INVALID Universe input.'))
    INVALID_UNIVERSE_SYMBOL = UniverseException(error(500, '[UniverseException] INVALID Universe symbol.'))

    INVALID_SIM_PARAMS = TradingException(error(500, '[TradingException] Parameters must be a'
                                                     'SimulationParameter instance.'))
    INVALID_COMMISSION = TradingException(error(500, '[TradingException] INVALID Commission instance.'))
    INVALID_ORDER_AMOUNT = TradingException(error(500, '[TradingException] INVALID order amount.'))
    INVALID_ORDER_STATE = TradingException(error(500, '[TradingException] INVALID order state.'))
    SWITCH_POSITION_FAILED = TradingException(error(500, '[TradingException] Switch position failed.'))

    INVALID_ACCOUNT_TYPE = AccountException(error(500, '[AccountException] INVALID account type.'))
    INVALID_ACCOUNT_NAME = AccountException(error(500, '[AccountException] INVALID account name.'))
    GET_ACCOUNT_ERROR = AccountException(error(500, '[AccountException] Can not get account. '))

    INVALID_ASSET_SYMBOL = DataLoadException(error(500, '[DataLoadException] INVALID asset in AssetService.'))
    DATA_API_ERROR = DataLoadException(error(500, '[DataLoadException] Data API error.'))
    DATA_NOT_AVAILABLE = DataLoadException(error(500, '[DataLoadException] Data loading error.'))

    DUPLICATE_ORDERS = InternalCheckException(error(500, '[InternalCheckException] Duplicated order exists.'))

    INVALID_STRATEGY_INITIALIZE = StrategyException(error(500, '[StrategyException] "initialize" must be a '
                                                               'callable function.'))
    INVALID_STRATEGY_HANDLE_DATA = StrategyException(error(500, '[StrategyException] "handle_data" must be a '
                                                                'callable function'))

    INVALID_COMMISSION_UNIT = TradingItemException(error(500, '[TradingItemException] Commission unit must be '
                                                              'either perValue or perShare.'))
    INVALID_SLIPPAGE_UNIT = TradingItemException(error(500, '[TradingItemException] Slippage unit must be '
                                                            'either perValue or perShare.'))
    INVALID_DIVIDEND = TradingItemException(error(500, '[TradingItemException] INVALID dividend.'))
    INVALID_ALLOT = TradingItemException(error(500, '[TradingItemException] INVALID allot.'))

    INVALID_INFO_MESSAGE = LogException(error(500, '[LogException] INVALID "INFO" message.'))
    INVALID_DEBUG_MESSAGE = LogException(error(500, '[LogException] INVALID "DEBUG" message.'))
    INVALID_WARN_MESSAGE = LogException(error(500, '[LogException] INVALID "WARN" message.'))
    INVALID_ERROR_MESSAGE = LogException(error(500, '[LogException] INVALID "ERROR" message.'))
    INVALID_FATAL_MESSAGE = LogException(error(500, '[LogException] INVALID "FATAL" message.'))
    INVALID_OTHER_MESSAGE = LogException(error(500, '[LogException] INVALID message.'))
    SAVE_INFO_MESSAGE_ERROR = LogException(error(500, '[LogException] "INFO" message save failed.'))
    SAVE_DEBUG_MESSAGE_ERROR = LogException(error(500, '[LogException] "DEBUG" message save failed.'))
    SAVE_WARN_MESSAGE_ERROR = LogException(error(500, '[LogException] "WARN" message save failed.'))
    SAVE_ERROR_MESSAGE_ERROR = LogException(error(500, '[LogException] "ERROR" message save failed.'))
    SAVE_FATAL_MESSAGE_ERROR = LogException(error(500, '[LogException] "FATAL" message save failed.'))
    SAVE_OTHER_MESSAGE_ERROR = LogException(error(500, '[LogException] Message save failed.'))

    INVALID_OPERATOR_INPUT = TradingInputException(error(500, '[TradingException] Universe operator input is invalid.'))

    @classmethod
    def enumerates(cls):
        return [value for attr, value in cls.__dict__.iteritems()]

    @classmethod
    def error_types(cls):
        return tuple([
            Exception, ImportError, NotImplementedError,
            SchemaException, DatabaseException, TradingException,
            TradingInputException, UniverseException,
            AccountException, DataLoadException, ContextException,
            HistoryException, TradingItemException, LogException
                      ])
