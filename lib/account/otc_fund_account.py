# -*- coding: utf-8 -*-
# **********************************************************************************#
#     File: OTCFundAccount FILE
# **********************************************************************************#
from . import BaseAccount
from .. trade import OTCFundOrder
from .. const import PURCHASE_CONFIRMING_DURATION, REDEEM_CONFIRMING_DURATION


# todo. need to adapt to base_account.


class OTCFundAccount(BaseAccount):
    """
    场外基金账户
    """
    def __init__(self, clock, data_portal, is_backtest=True):
        super(OTCFundAccount, self).__init__(clock=clock,
                                             data_portal=data_portal,
                                             is_backtest=is_backtest)
        self.account_type = 'otc_fund'

    def purchase(self, symbol, order_capital):
        """
        申购基金

        Args:
            symbol (str): 待申购的基金代码
            order_capital (float): 待申购的金额

        Returns:
            str: 申购委托的订单 ID
        """
        order_time = self.clock.with_(hour=0, minute=0, second=0)
        purchase_confirming_date = self.data_portal.calendar_service.get_direct_trading_day(order_time,
                                                                                            PURCHASE_CONFIRMING_DURATION,
                                                                                            forward=True)
        order = OTCFundOrder(symbol, 'purchase', order_capital=order_capital, order_time=order_time,
                             purchase_confirming_date=purchase_confirming_date)
        self.submitted_orders.append(order)
        return order.order_id

    def redeem(self, symbol, order_amount):
        """
        赎回基金

        Args:
            symbol (str): 待赎回的基金代码
            order_amount (float): 待赎回的份额

        Returns:
            str: 赎回委托的订单 ID
        """
        order_time = self.clock.with_(hour=0, minute=0, second=0)
        redeem_confirming_date = self.data_portal.calendar_service.get_direct_trading_day(order_time,
                                                                                          REDEEM_CONFIRMING_DURATION,
                                                                                          forward=True)
        order = OTCFundOrder(symbol, 'redeem', order_amount=order_amount, order_time=order_time,
                             redeem_confirming_date=redeem_confirming_date)
        self.submitted_orders.append(order)
        return order.order_id

    # def get_orders(self, state=OrderState.ACTIVE, symbol='all', **kwargs):
    #     """
    #     通过订单状态查询当亲满足要求的订单，当前支持的订单状态可详见文档。
    #
    #     Args:
    #         state (OrderState): 订单状态详见文档
    #         symbol (str or list): 订单所属证券或证券列表，可用'all'表示所有订单
    #
    #     Returns:
    #         list: 满足 state 的 Order 对象列表
    #     """
    #     status = state if state else kwargs.get('status', OrderState.ACTIVE)
    #     symbol = [symbol] if not isinstance(symbol, list) and symbol is not 'all' else symbol
    #     status = [status] if not isinstance(status, list) else status
    #     if not set(status).issubset(set(OrderState.ALL)):
    #         raise ValueError('Invalid OrderStatus %s, '
    #                          'please refer to document for more information' % '/'.join(status))
    #     state_orders = self._broker.blotter.get_by_status(status)
    #     return [self.get_order(order_id) for order_id in state_orders if symbol == 'all'
    #             or self.get_order(order_id).symbol in symbol]

    def get_position(self, symbol):
        """
        返回某只证券的持仓明细

        Args:
            symbol: 具体证券代码

        Returns:
            Position: 返回该证券代码所对应的 Position 对象; 若不包含该持仓，返回 None
        """
        return self.position.get(symbol, None)

    def get_positions(self):
        """
        返回当前账户的持仓明细

        Returns:
            dict: 返回当前所有持仓，key 为证券代码，value 为对应的 Position 对象
        """
        return self.position

    def close_all_positions(self, symbol=None):
        """
        对场外基金进行全部赎回

        Args:
            symbol(basestring or list): Optional, 具体证券代码或证券代码列表

        """
        symbols = symbol if symbol is not None else self.get_positions().keys()
        symbols = symbols if isinstance(symbols, list) else [symbols]
        for otc_fund in symbols:
            position = self.get_position(otc_fund)
            if position and position.available_amount:
                self.redeem(otc_fund, position.available_amount)

    def get_trades(self, order_id=None):
        """
        返回当前账户的交易明细

        Args:
            order_id (str): optional, if str,  返回当前 order_id 对应的 OTCFundTrade 对象，若找不到，返回 None；
                                     if None, 返回当天 trades 列表
        Returns:
            list: 当日 trades 列表
        """
        if order_id:
            for trade in self.portfolio_info['trades']:
                if trade.order_id == order_id:
                    return trade
            return None
        return self.portfolio_info['trades']
