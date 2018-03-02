# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: XDAEX exchange gateway.
# **********************************************************************************#
from configs import *
from . base_gateway import BaseGateway
from .. event.event_engine import EventType, EventEngine


class Gateway(BaseGateway):

    def __init__(self, event_engine):
        super(Gateway, self).__init__(self)
        self.event_engine = event_engine
        self.gateway_name = 'Gateway'

    @classmethod
    def with_event_engine(cls):
        """
        Generate gateway with event engine initialization.
        """
        event_engine = EventEngine()
        instance = cls(event_engine)
        for event in EventType.registered_events():
            event_engine.register_handlers(event, getattr(instance, event))
        return instance

    def on_tick(self, strategy, context, tick, **kwargs):
        """
        On tick response
        """
        if hasattr(strategy, 'on_tick'):
            strategy.on_tick(context, tick)

    def on_order(self, strategy, context, order, **kwargs):
        """
        On order response
        """
        if hasattr(strategy, 'on_order'):
            strategy.on_order(context, order)

    def on_trade(self, strategy, context, trade, **kwargs):
        """
        On tick response
        """
        if hasattr(strategy, 'on_trade'):
            strategy.on_trade(context, trade)

    def on_order_book(self, strategy, context, order_book, **kwargs):
        """
        On order book response
        """
        if hasattr(strategy, 'on_order_book'):
            strategy.on_order_book(context, order_book)

    def handle_data(self, strategy, context, **kwargs):
        """
        Handle data response
        """
        if hasattr(strategy, 'handle_data'):
            strategy.handle_data(context)

    def on_log(self, strategy, context, log, **kwargs):
        """
        On log response
        """
        if hasattr(strategy, 'on_log'):
            strategy.on_log(context, log)

    @staticmethod
    def send_order(order, account_id=None, headers=None):
        """
        Send order.

        Args:
            order(obj): order object
            account_id(string): account id
            headers(obj): http headers
        """
        logger.info('[Send order] account_id: {}, order_id: {}'.format(account_id, order.order_id))

    @staticmethod
    def cancel_order(order_id, account_id=None, headers=None):
        """
        Send cancel order.
        Args:
            order_id(string): order id
            account_id(string): account id
            headers(obj): http headers
        """
        logger.info('[Cancel order] account_id: {}, order_id: {}'.format(account_id, order_id))
