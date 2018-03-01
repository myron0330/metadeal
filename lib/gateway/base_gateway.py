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

    def __init__(self, event_engine):
        self.event_engine = event_engine
        self.gateway_name = 'Base_Gateway'

    def start(self):
        """
        Start
        """
        self.event_engine.start()

    def stop(self):
        """
        Stop
        """
        self.event_engine.stop()

    def publish(self, event_type, **kwargs):
        """
        Publish event
        Args:
            event_type(string): event type
            **kwargs: key-value parameters
        """
        event = Event(event_type=event_type, **kwargs)
        self.event_engine.put_event(event)

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
