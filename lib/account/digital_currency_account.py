# -*- coding: utf-8 -*-
# **********************************************************************************#
#     File: Digital Currency Account FILE
# **********************************************************************************#
import numpy as np
from . base_account import BaseAccount
from .. trade import (
    DigitalCurrencyOrder,
    DigitalCurrencyPosition,
    OrderState
)
from .. event.event_base import EventType
from .. utils.error_utils import (
    Errors
)


class DigitalCurrencyAccount(BaseAccount):
    """
    Digital currency account.
    """
    def __init__(self, clock, data_portal, event_engine=None, pms_gateway=None, account_id=None):
        super(DigitalCurrencyAccount, self).__init__(clock=clock,
                                                     data_portal=data_portal,
                                                     event_engine=event_engine,
                                                     pms_gateway=pms_gateway,
                                                     account_id=account_id)
        self.account_type = 'digital_currency'
        self.record = dict()

    def order(self, symbol, amount, price=0., order_type='market', **kwargs):
        """
        Send order.

        Args:
            symbol (str): 需要交易的证券代码，必须包含后缀，其中上证证券为.XSHG，深证证券为.XSHE
            amount (float or int): 需要交易的证券代码为symbol的证券数量，为正则为买入，为负则为卖出；程序会自动对amount向下取整到最近的整百
            price (float): 交易指令限价（仅分钟线策略可用）
            order_type (str): 交易指令类型，可以为'market'或'limit'，默认为'market'，为limit时仅日内策略可用
        """
        if not isinstance(amount, (int, long, float)) or np.isnan(amount):
            raise Errors.INVALID_ORDER_AMOUNT
        if amount == 0 or np.isinf(amount):
            return
        timestamp = str(self.clock.now)
        order = DigitalCurrencyOrder(symbol, amount, order_time=timestamp, order_type=order_type, price=price, **kwargs)
        parameters = {
            'order': order,
            'account_id': self.account_id,
        }
        self.event_engine.publish(EventType.event_send_order, **parameters)
        return order.order_id

    def cancel_order(self, order_id):
        """
        Cancel order.

        Args:
            order_id(string): order id.
        """
        parameters = {
            'order_id': order_id,
            'account_id': self.account_id,
        }
        self.event_engine.publish(EventType.event_cancel_order, **parameters)
        return order_id

    def get_positions(self):
        """
        Get all positions dict.
        """
        position_list = self.pms_gateway.get_positions(self.account_id)
        positions = {
            item['currency']: DigitalCurrencyPosition.from_subscribe(item) for item in position_list
        }
        return positions

    def get_position(self, currency, **kwargs):
        """
        Get position of a specific currency.

        Args:
            currency(string): currency name

        Returns:
            Position: Position object.
        """
        position_list = self.pms_gateway.get_positions(self.account_id)
        target_position_list = filter(lambda x: x['currency'] == currency, position_list)
        if not target_position_list:
            return None
        target_position = DigitalCurrencyPosition.from_subscribe(target_position_list[0])
        return target_position

    def get_trades(self):
        """
        Get trades list in current date.
        """
        return self.pms_gateway.get_trades(self.account_id)

    def get_orders(self, state=OrderState.ACTIVE, symbol='all', **kwargs):
        """
        Get orders by state and symbol.

        Args:
            state(string): order state.
            symbol(string): currency pair symbol

        Returns:
            list: qualified order list
        """
        status = state if state else kwargs.get('status', OrderState.ACTIVE)
        symbol = [symbol] if not isinstance(symbol, list) and symbol is not 'all' else symbol
        status = [status] if not isinstance(status, list) else status
        if not set(status).issubset(set(OrderState.ALL)):
            raise ValueError('Invalid OrderStatus %s, '
                             'please refer to document for more information' % '/'.join(status))
        orders = self.pms_gateway.get_orders(self.account_id)
        return [order for order_id, order in orders.iteritems()
                if order.state in status and symbol == 'all' or order.symbol in symbol]

    def get_order(self, order_id):
        """
        Get order by order id.

        Args:
            order_id(string): order_id

        Returns:
            Order or None: Order instance if order id exists, else None
        """
        order = self.pms_gateway.get_orders(self.account_id).get(order_id)
        return order
