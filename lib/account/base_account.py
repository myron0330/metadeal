# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: BaseAccount FILE
# **********************************************************************************#
from abc import ABCMeta, abstractmethod
from utils.error_utils import Errors
from utils.dict_utils import CompositeDict
from .. context.parameters import SimulationParameters
from .. trade.order import OrderState


class BaseAccount(object):

    __metaclass__ = ABCMeta

    def __init__(self, clock=None, data_portal=None):
        self.clock = clock
        self.data_portal = data_portal
        self.portfolio_info = CompositeDict()
        self.submitted_orders = list()
        self.submitted_cancel_orders = list()

    @classmethod
    def from_config(cls, clock, sim_params, data_portal):
        """
        Generate account from global configs

        Args:
            clock(Clock): global clock
            sim_params(SimulationParameters): simulation parameters
            data_portal(DataPortal): data portal service
        """
        if not isinstance(sim_params, SimulationParameters):
            raise Errors.INVALID_SIM_PARAMS
        return cls(clock, data_portal)

    @property
    def cash(self):
        """
        Current cash
        """
        return self.portfolio_info['cash']

    @property
    def positions(self):
        """
        Current positions.
        """
        return self.portfolio_info['positions']

    @property
    def portfolio_value(self):
        """
        Portfolio value
        """
        return self.portfolio_info['portfolio_value']

    def get_order(self, order_id):
        """
        Get order by order ID.

        Args:
            order_id(string): order_id

        Returns:
            Order or None: Order instance or None.
        """
        order = self.portfolio_info['orders'].get(order_id)
        if order is None:
            submitted = filter(lambda x: x.order_id == order_id, self.submitted_orders)
            if len(submitted) == 1:
                return submitted[0]
            else:
                raise Errors.DUPLICATE_ORDERS
        return order

    def get_orders(self, state=OrderState.ACTIVE, symbol='all', **kwargs):
        """
        Get orders by order state and symbol.
        Args:
            state(string): OrderState
            symbol(string or list): specified order or order list, support 'all'

        Returns:
            list: satisfied order list
        """
        symbol = [symbol] if not isinstance(symbol, list) and symbol is not 'all' else symbol
        state = [state] if not isinstance(state, list) else state
        if not set(state).issubset(set(OrderState.ALL)):
            raise Errors.INVALID_ORDER_STATE
        return [order for order_id, order in self.portfolio_info['orders'].iteritems()
                if order.state in state and symbol == 'all' or order.symbol in symbol]

    def cancel_order(self, order_id):
        """
        Cancel order.
        Args:
            order_id(string): order ID.
        Returns:
            string: order_id
        """
        self.submitted_cancel_orders.append(order_id)
        return order_id

    def get_trades(self):
        """
        Get trades list in current date.
        """
        return self.portfolio_info['trades']

    @abstractmethod
    def get_position(self, *args, **kwargs):
        """
        Get a specific position object.
        """
        return

    @abstractmethod
    def get_positions(self, *args, **kwargs):
        """
        Get all positions dict.
        """
        return
