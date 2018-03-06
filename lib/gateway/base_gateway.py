# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Trading gateway.
# **********************************************************************************#
from abc import ABCMeta
from .. event.event_base import Event


class BaseGateway(object):
    """
    Trading gateway interface
    """
    __metaclass__ = ABCMeta

    def __init__(self, gateway_name='Base_Gateway'):
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

    def connect(self, *args, **kwargs):
        """
        Connect to exchange
        """
        pass

    def subscribe(self):
        """
        Subscribe market quote
        """
        pass

    def query_account(self):
        """
        Query account info
        """
        pass

    def query_position(self):
        """
        Query position info
        """
        pass
