# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Trading gateway.
# **********************************************************************************#
from abc import ABCMeta
from .. event.event_base import Event


class BaseStrategyGateway(object):
    """
    Base Strategy gateway interface.
    """
    __metaclass__ = ABCMeta

    def __init__(self, gateway_name='Strategy_Gateway'):
        self.gateway_name = gateway_name

    def on_tick(self, *args, **kwargs):
        """
        On tick response
        """
        raise NotImplementedError

    def on_order(self, *args, **kwargs):
        """
        On order response
        """
        raise NotImplementedError

    def on_trade(self, *args, **kwargs):
        """
        On tick response
        """
        raise NotImplementedError

    def on_order_book(self, *args, **kwargs):
        """
        On order book response
        """
        raise NotImplementedError

    def handle_data(self, *args, **kwargs):
        """
        Handle data response
        """
        raise NotImplementedError

    def on_log(self, *args, **kwargs):
        """
        On log response
        """
        pass

    def send_order(self, *args, **kwargs):
        """
        Send order to exchange
        """
        raise NotImplementedError

    def cancel_order(self, *args, **kwargs):
        """
        Send cancel order to exchange
        """
        raise NotImplementedError


class BasePMSGateway(object):
    """
    Base PMS gateway interface.
    """
    __metaclass__ = ABCMeta

    def __init__(self, gateway_name='PMS_Gateway'):
        self.gateway_name = gateway_name

    def send_order(self, *args, **kwargs):
        """
        Send order to exchange
        """
        raise NotImplementedError

    def cancel_order(self, *args, **kwargs):
        """
        Send cancel order to exchange
        """
        raise NotImplementedError

    def deal_with_trade(self, *args, **kwargs):
        """
        Deal with trade response
        """
        raise NotImplementedError

    def deal_with_order(self, *args, **kwargs):
        """
        Deal with order response
        """
        raise NotImplementedError


class BaseSubscriberGateway(object):
    """
    Base Subscriber gateway interface.
    """
    __metaclass__ = ABCMeta

    def __init__(self, gateway_name='Subscriber_Gateway'):
        self.gateway_name = gateway_name

    def subscribe_trade(self, *args, **kwargs):
        """
        Subscribe trade.
        """
        raise NotImplementedError


class BaseTradingGateway(object):
    """
    Base Trading gateway interface.
    """
    __metaclass__ = ABCMeta

    def __init__(self, gateway_name='Trading_Gateway'):
        self.gateway_name = gateway_name

    def start(self):
        """
        Start
        """
        raise NotImplementedError

    def stop(self):
        """
        Stop
        """
        raise NotImplementedError

    def on_bar(self, *args, **kwargs):
        """
        On bar response
        """
        raise NotImplementedError

    def on_portfolio(self, *args, **kwargs):
        """
        On portfolio response
        """
        raise NotImplementedError
