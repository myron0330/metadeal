# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: PMS broker: broker pms_agent for brokers in PMS.
# **********************************************************************************#
from . base import list_wrap_
from . broker.futures_broker import FuturesBroker
from . broker.security_broker import SecurityBroker
from lib.gateway.subscriber import MarketQuote
from .. const import (
    STOCK_PATTERN,
    BASE_FUTURES_PATTERN,
    INDEX_PATTERN
)
from .. core.enum import SecuritiesType
from .. utils.error_utils import Errors


market_quote = MarketQuote.get_instance()


def traversal_broker(func):
    """
    Traversal brokers to do a specific task.

    Args:
        func(obj): function
    """

    def _decorator(obj, *args, **kwargs):
        """
        Decorator: doing brokers traversal
        """
        securities_type_list = \
            list_wrap_(kwargs.get('securities_type', SecuritiesType.ALL))
        for _, broker in obj.pms_brokers.iteritems():
            if _ in securities_type_list and hasattr(broker, func.func_name):
                getattr(broker, func.func_name)(*args, **kwargs)
    return _decorator


class BrokerMcs(type):
    """
    Broker meta class
    """
    def __new__(mcs, name, bases, attributes):
        pms_brokers = {key: value for key, value in attributes.iteritems() if key.endswith('_broker')}
        attributes['pms_brokers'] = {name.replace('_broker', ''): attributes[name] for name in pms_brokers}
        return type.__new__(mcs, name, bases, attributes)


class PMSBroker(object):
    """
    PMSBroker: brokers agent for managing all brokers in PMS.
    """
    __metaclass__ = BrokerMcs

    security_broker = SecurityBroker()
    futures_broker = FuturesBroker()

    def __new__(cls, *args, **kwargs):
        """
        Single instance pms broker
        """
        if not hasattr(cls, '_instance'):
            cls._instance = super(PMSBroker, cls).__new__(cls)
        return cls._instance

    @traversal_broker
    def prepare(self, *args, **kwargs):
        """
        Prepare at the beginning for all brokers
        """
        pass

    @traversal_broker
    def post_trading_day(self, *args, **kwargs):
        """
        Post trading day tasks for all brokers
        """
        pass

    def accept_orders(self, orders, securities_type=SecuritiesType.SECURITY):
        """
        Accept orders according to securities types
        """
        self.pms_brokers[securities_type].accept_orders(orders)

    def get_pre_close_price_of_(self, symbol):
        """
        Get symbol price

        Args:
            symbol(string): symbol

        Returns:
            float: price
        """
        if STOCK_PATTERN.match(symbol) or INDEX_PATTERN.match(symbol):
            price = self.security_broker.get_pre_close_price().get(symbol)
        elif BASE_FUTURES_PATTERN.match(symbol):
            price = self.futures_broker.get_pre_settlement_price().get(symbol)
        else:
            raise Errors.INVALID_SECURITIES_TYPE
        return price

    def playback_daily(self, order_schema, position_schema, securities_type=SecuritiesType.SECURITY):
        """
        Playback in daily market

        Args:
            order_schema(obj): order schema
            position_schema(obj): position schema
            securities_type(string): securities type
        """
        daily_data = market_quote.get_current_daily_bar_info(security_type=securities_type)
        return self.pms_brokers[securities_type].playback_daily(order_schema, position_schema, daily_data)
