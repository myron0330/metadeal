# -*- coding: utf-8 -*-
# **********************************************************************************#
#     File: FuturesAccount FILE
#   Author: Myron
# **********************************************************************************#
from base_account import BaseAccount
from .. data_loader.cache_api import *
from .. trade import (
    FuturesOrder, OrderState,
)
from .. utils.error_utils import (
    WARNER,
    BacktestInputError,
    HandleDataError,
    HandleDataMessage,
)


class FuturesAccount(BaseAccount):

    def __init__(self, clock, data_portal, is_backtest=True):
        super(FuturesAccount, self).__init__(clock=clock,
                                             data_portal=data_portal,
                                             is_backtest=is_backtest)
        self.account_type = 'futures'

    def order(self, symbol, amount, direction=None, offset_flag="open", order_type="market", price=0.):
        """
        订单委托.交易品种只能是期货合约.

        Args:
            symbol (str): 期货合约代码
            amount (float): 下单数量
            direction (int or str): optional, 下单方向，1 --> 多， -1 --> 空
            offset_flag (str): optional, 开平仓, 'open' --> 开仓， 'close' --> 平仓
            order_type (str): optional, 'market' --> 市价单， 'limit' --> 限价单
            price (float): optional, 限价单价格
        """
        # 兼容之前版本的下单方式
        direction_offset_map = {
            ('long', 'open'): ('buy', 'open'),
            ('long', 'close'): ('sell', 'close'),
            ('short', 'open'): ('sell', 'open'),
            ('short', 'close'): ('buy', 'close'),
        }
        direction, offset_flag = \
            direction_offset_map.get((direction, offset_flag), (direction, offset_flag))

        # 兼容函数签名档顺序
        if direction in ['open', 'close']:
            offset_flag = direction
            direction = None

        if not isinstance(amount, (int, long, float)):
            raise BacktestInputError("Order amount must be integer or float number!")
        if np.isnan(amount):
            raise BacktestInputError("Order amount is nan!")
        if order_type not in ['market', 'limit']:
            msg = HandleDataMessage.ORDER_TYPE_ERROR.format(order_type)
            raise HandleDataError(msg)
        elif amount > 0:
            amount = int(amount)
            # 遵从传入的买卖方向
            direction = direction if direction else 'buy'
        elif amount < 0:
            amount = abs(int(amount))
            # 遵从传入的买卖方向
            direction = direction if direction else 'sell'

        if direction not in ['buy', 'sell']:
            raise BacktestInputError('Exception in "FuturesAccount.order": '
                                     'order direction is not correct. [hint:%s]' % direction)

        if offset_flag not in ['open', 'close']:
            # raise BacktestInputError('Exception in "FuturesAccount.order": '
            #                          'order offset_flag is not correct. [hint:%s]' % offset_flag)
            msg = HandleDataMessage.OFFSET_FLAG_ERROR.format(offset_flag)
            raise HandleDataError(msg)
        if order_type not in ['market', 'limit']:
            raise BacktestInputError('Exception in "FuturesAccount.order":'
                                     'order type is not correct. [hint:%s]' % order_type)

        if not symbol:
            return

        order_time = self._get_order_time()
        direction_map = {
            'buy': 1,
            'sell': -1
        }

        order_type = 'limit' if price else order_type
        order = FuturesOrder(symbol=symbol, order_amount=amount, offset_flag=offset_flag, order_type=order_type,
                             price=price, direction=direction_map.get(direction, 1), order_time=order_time,
                             state=OrderState.ORDER_SUBMITTED)
        self.submitted_orders.append(order)
        return order.order_id

    def close_all_positions(self, symbol=None):
        """
        对期货进行全部平仓

        Args:
            symbol(basestring or list): Optional, 具体期货代码或期货代码列表

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
        返回某只证券的持仓明细

        Args:
            symbol: 具体证券代码

        Returns:
            Position: 返回该证券代码所对应的 Position 对象; 若不包含该持仓，返回 None
        """
        position = self.position.get(symbol, None)
        if position and (position.long_amount or position.short_amount):
            return position
        return None

    def get_positions(self):
        """
        返回当前账户的持仓明细

        Returns:
            dict: 返回当前所有持仓，key 为证券代码，value 为对应的 Position 对象
        """
        return {
            symbol: position for symbol, position in self.position.iteritems()
            if position.long_amount or position.short_amount
        }

    def switch_position(self, symbol_from=None, symbol_to=None):
        """
        在人工合约切换的场景下，切换原有持仓合约(symbol_from)至新合约(symbol_to)。

        Args:
            symbol_from(str): 原期货合约symbol
            symbol_to(str): 新期货合约symbol

        """
        futures_universe = self.data_portal.market_service.futures_market_data.universe
        # 校验两个非空, 且symbol_from, symbol_to在当前行情中
        if symbol_from not in futures_universe or symbol_to not in futures_universe:
            raise BacktestInputError('Exception in "FuturesAccount.switch_position":'
                                     'Symbol_from or symbol_to not in current universe.')
        symbol_holding = self.position.get(symbol_from)
        if symbol_holding is None:
            long_amount, short_amount = (0, 0)
        else:
            long_amount, short_amount = symbol_holding['long_amount'], symbol_holding['short_amount']
        if (long_amount, short_amount) == (0, 0):
            message = "Warning: Account has no {} holding on trading day {}, skip position switching.".format(
                symbol_from, self.clock.current_date)
            WARNER.warn(message)
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
