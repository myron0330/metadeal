# -*- coding: utf-8 -*-
# **********************************************************************************#
#     File: IndexFundAccount FILE
# **********************************************************************************#
from . base_account import BaseAccount
from .. trade import OrderState, IndexOrder
from .. utils.error_utils import (
    BacktestInputError,
    HandleDataError,
    HandleDataMessage,
)


class IndexAccount(BaseAccount):

    def __init__(self, clock, data_portal, is_backtest=True):
        super(IndexAccount, self).__init__(clock=clock,
                                           data_portal=data_portal,
                                           is_backtest=is_backtest)
        self.account_type = 'index'

    def order(self, symbol, amount, direction=None, offset_flag=None, order_type="market", **kwargs):
        """
        下达指数订单

        Args:
            symbol(str): index symbol
            amount(float): amount
            direction(string): 'buy', 'sell'
            offset_flag(string): 'open', 'close'
            order_type(string): 'market', 'limit'

        Returns:
            str: order_id
        """
        # 兼容之前版本的下单方式
        # direction_offset_map = {
        #     ('long', 'open'): ('buy', 'open'),
        #     ('long', 'close'): ('sell', 'close'),
        #     ('short', 'open'): ('sell', 'open'),
        #     ('short', 'close'): ('buy', 'close'),
        # }
        # direction, offset_flag = direction_offset_map.get((direction, offset_flag), (direction, offset_flag))

        # 兼容函数签名档顺序
        if direction in ['open', 'close']:
            offset_flag = direction
            direction = None

        if not isinstance(amount, (int, long, float)):
            raise BacktestInputError("Order amount must be integer or float number!")
        elif amount > 0:
            amount = int(amount)
            # 遵从传入的买卖方向
            direction = direction if direction else 'buy'
        elif amount < 0:
            amount = abs(int(amount))
            # 遵从传入的买卖方向
            direction = direction if direction else 'sell'

        if direction not in ['buy', 'sell']:
            raise BacktestInputError('Exception in "IndexAccount.order": '
                                     'order direction is not correct. [hint:%s]' % direction)
        if offset_flag not in ['open', 'close']:
            # raise BacktestInputError('Exception in "IndexAccount.order": '
            #                          'order offset_flag is not correct. [hint:%s]' % offset_flag)
            msg = HandleDataMessage.OFFSET_FLAG_ERROR.format(offset_flag)
            raise HandleDataError(msg)
        if order_type != 'market':
            raise BacktestInputError('Exception in "IndexAccount.order":'
                                     'order type should be market only. [hint:%s]' % order_type)
        if not symbol:
            return

        order_time = self.clock.now
        direction_map = {
            'buy': 1,
            'sell': -1
        }

        order = IndexOrder(symbol=symbol, order_amount=amount, order_type=order_type, offset_flag=offset_flag,
                           direction=direction_map.get(direction, 1), order_time=order_time,
                           state=OrderState.ORDER_SUBMITTED)
        self.submitted_orders.append(order)
        return order.order_id

    def get_position(self, symbol):
        """
        Get specific position object.
        Args:
            symbol(string): index symbol

        Returns:
            IndexPosition: index position object
        """
        return self.position.get(symbol, None)

    def get_positions(self):
        """
        返回当前账户的持仓明细

        Returns:
            dict: 返回当前所有持仓，key 为证券代码，value 为对应的 Position 对象
        """
        return self.position

    def get_trades(self, order_id=None):
        """
        返回当前账户的交易明细

        Args:
            order_id (str): optional, if str,  返回当前 order_id 对应的 OTCFundTrade 对象，若找不到，返回 None；
                                     if None, 返回当天 trades 列表
        Returns:
            list: 当日 trades 列表
        """
        raise NotImplementedError
        # if order_id:
        #     for trade in self._broker.trades:
        #         if trade.order_id == order_id:
        #             return trade
        #     return None
        # return self._broker.trades

    def close_all_positions(self, symbol=None):
        """
        对指数进行全部赎回

        Args:
            symbol(basestring or list): Optional, 具体证券代码或证券代码列表

        """
        symbols = symbol if symbol is not None else self.get_positions().keys()
        symbols = symbols if isinstance(symbols, list) else [symbols]
        for index in symbols:
            position = self.get_position(index)
            if position:
                if position.long_amount:
                    self.order(index, position.long_amount, direction='sell', offset_flag='close')
                if position.short_amount:
                    self.order(index, position.short_amount, direction='buy', offset_flag='close')
