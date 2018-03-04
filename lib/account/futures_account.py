# -*- coding: utf-8 -*-
# **********************************************************************************#
#     File: FuturesAccount FILE
#   Author: Myron
# **********************************************************************************#
from utils.error_utils import Errors
from . base_account import BaseAccount
from .. trade import (
    Order, OrderState,
)


class FuturesAccount(BaseAccount):

    def __init__(self, clock, data_portal):
        super(FuturesAccount, self).__init__(clock=clock,
                                             data_portal=data_portal)
        self.account_type = 'futures'

    def order(self, symbol, amount, direction=None, offset_flag="open", order_type="market", price=0.):
        """
        Order.

        Args:
            symbol(string): futures symbol
            amount(float): order amount
            direction(int or str): optional, 1 --> long, -1 --> short
            offset_flag(str): optional, 'open' --> open， 'close' --> close
            order_type(str): optional, 'market' --> market price， 'limit' --> limit price
            price(float): optional, limit price definition
        """
        direction_offset_map = {
            ('long', 'open'): ('buy', 'open'),
            ('long', 'close'): ('sell', 'close'),
            ('short', 'open'): ('sell', 'open'),
            ('short', 'close'): ('buy', 'close'),
        }
        direction, offset_flag = \
            direction_offset_map.get((direction, offset_flag), (direction, offset_flag))

        if direction in ['open', 'close']:
            offset_flag = direction
            direction = None

        if not symbol:
            return

        order_time = self._get_order_time()
        direction_map = {
            'buy': 1,
            'sell': -1
        }

        order_type = 'limit' if price else order_type
        order = Order(symbol=symbol, order_amount=amount, offset_flag=offset_flag, order_type=order_type,
                      price=price, direction=direction_map.get(direction, 1), order_time=order_time,
                      state=OrderState.ORDER_SUBMITTED)
        self.submitted_orders.append(order)
        return order.order_id

    def close_all_positions(self, symbol=None):
        """
        Close all positions by symbols

        Args:
            symbol(string or list): Optional, specific futures symbol or symbol list
        """
        symbols = symbol if symbol is not None else self.get_positions().keys()
        symbols = symbols if isinstance(symbols, list) else [symbols]
        for future in symbols:
            position = self.get_position(future)
            if position:
                if position.long_amount:
                    self.order(future, position.long_amount, direction='sell',  offset_flag='close')
                if position.short_amount:
                    self.order(future, position.short_amount, direction='buy',  offset_flag='close')

    def get_position(self, symbol):
        """
        Get position by symbol.

        Args:
            symbol(string): symbol

        Returns:
            Position(Object):
        """
        position = self.positions.get(symbol, None)
        if position and (position.long_amount or position.short_amount):
            return position
        return None

    def get_positions(self):
        """
        Get positions

        Returns:
            dict: all positions.
        """
        return {
            symbol: position for symbol, position in self.positions.iteritems()
            if position.long_amount or position.short_amount
        }

    def switch_position(self, symbol_from=None, symbol_to=None):
        """
        Switch position.

        Args:
            symbol_from(string): original symbol
            symbol_to(string): substituted symbol

        """
        futures_universe = self.data_portal.market_service.futures_market_data.universe
        if symbol_from not in futures_universe or symbol_to not in futures_universe:
            raise Errors.SWITCH_POSITION_FAILED
        symbol_holding = self.positions.get(symbol_from)
        if symbol_holding is None:
            long_amount, short_amount = (0, 0)
        else:
            long_amount, short_amount = symbol_holding['long_amount'], symbol_holding['short_amount']
        if (long_amount, short_amount) == (0, 0):
            return
        else:
            if long_amount > 0:
                self.order(symbol_from, long_amount, 'sell', 'close')
                self.order(symbol_to, long_amount, 'buy', 'open')
            if short_amount > 0:
                self.order(symbol_from, short_amount, 'buy', 'close')
                self.order(symbol_to, short_amount, 'sell', 'open')

    def _get_order_time(self):
        """
        Get current order time based on clock and frequency.
        """
        return self.clock.now.strftime('%Y-%m-%d') \
            if self.clock.freq == 'd' else self.clock.now.strftime('%Y-%m-%d %H:%M')
