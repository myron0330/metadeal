# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: XDAEX exchange gateway.
# **********************************************************************************#
import requests
import traceback
from configs import *
from . base_gateway import BaseGateway
from .. utils.decorator_utils import mutex_lock


class StrategyGateway(BaseGateway):

    def __init__(self, gateway_name='Gateway'):
        super(StrategyGateway, self).__init__(self)
        self.gateway_name = gateway_name

    @mutex_lock
    def on_tick(self, strategy, context, tick, **kwargs):
        """
        On tick response
        """
        if hasattr(strategy, 'on_tick'):
            strategy.on_tick(context, tick)

    @mutex_lock
    def on_order(self, strategy, context, order, **kwargs):
        """
        On order response
        """
        if hasattr(strategy, 'on_order'):
            strategy.on_order(context, order)

    @mutex_lock
    def on_trade(self, strategy, context, trade, **kwargs):
        """
        On tick response
        """
        if hasattr(strategy, 'on_trade'):
            strategy.on_trade(context, trade)

    @mutex_lock
    def on_order_book(self, strategy, context, order_book, **kwargs):
        """
        On order book response
        """
        if hasattr(strategy, 'on_order_book'):
            strategy.on_order_book(context, order_book)

    @mutex_lock
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
    def send_order(order, account_id=None):
        """
        Send order.

        Args:
            order(obj): order object
            account_id(string): account id
        """
        create_order_url = '/'.join([order_url, 'create'])
        request_order = order.to_request()
        request_order['accountId'] = account_id
        logger.info('[Strategy Gateway] [Send order] account_id: {}, order_id: {}'.format(account_id, order.order_id))
        try:
            response = requests.post(create_order_url, json=request_order).json()
        except IOError:
            logger.info('[Strategy Gateway] [Send order] [IOError] account_id: {}, order_id: {}'.format(account_id, order.order_id))
            logger.info(traceback.format_exc())
        except:
            logger.info('[Strategy Gateway] [Send order] [Other Error] account_id: {}, order_id: {}'.format(account_id, order.order_id))
            logger.info(traceback.format_exc())
        else:
            logger.info('[Strategy Gateway] [Send order] [Response]: {}'.format(response))

    @staticmethod
    def cancel_order(order_id, account_id=None):
        """
        Send cancel order.
        Args:
            order_id(string): order id
            account_id(string): account id
        """
        cancel_order_url = '/'.join([order_url, 'cancel'])
        request_data = {
            'extOrdId': order_id,
            'accountId': account_id,
        }
        logger.info('[Strategy Gateway] [Cancel order] account_id: {}, order_id: {}'.format(account_id, order_id))
        try:
            response = requests.post(cancel_order_url, json=request_data)
        except IOError:
            logger.info('[Strategy Gateway] [Cancel order] [IOError] account_id: {}, order_id: {}'.format(account_id, order_id))
        except:
            logger.info('[Strategy Gateway] [Cancel order] [Other Error] account_id: {}, order_id: {}'.format(account_id, order_id))
            logger.info(traceback.format_exc())
        else:
            logger.info('[Strategy Gateway] [Cancel order] [Response]: {}'.format(response))
