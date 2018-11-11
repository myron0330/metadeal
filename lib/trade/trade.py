"""
# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: trade file.
#   Author: Myron
# **********************************************************************************#
"""
import uuid
from .. core.objects import ValueObject


class Trade(object):

    """
    股票、场内基金成交记录明细
    """

    def __init__(self, order_id, symbol, direction, offset_flag, filled_amount, transact_price, filled_time,
                 commission, slippage):
        self.order_id = order_id
        self.symbol = symbol
        self.direction = direction
        self.offset_flag = offset_flag
        self.filled_amount = filled_amount
        self.transact_price = transact_price
        self.filled_time = filled_time
        self.commission = commission
        self.slippage = slippage

    def to_dict(self):
        """
        To dict
        """
        return self.__dict__

    def __repr__(self):
        return "Trade(symbol: {}, direction: {}, offset_flag: {}, filled_amount: {}, transact_price: {}, " \
               "filled_time: {}, commission: {}, slippage: {})"\
            .format(self.symbol, self.direction, self.offset_flag, self.filled_amount, self.transact_price,
                    self.filled_time, self.commission, self.slippage)


class MetaTrade(Trade):
    """
    Meta trade
    """

    def __init__(self, order_id=None, symbol=None, direction=None, offset_flag=None,
                 filled_amount=None, transact_price=None, filled_time=None,
                 commission=None, slippage=None, portfolio_id=str(uuid.uuid1())):
        super(MetaTrade, self).__init__(order_id, symbol, direction, offset_flag, filled_amount,
                                        transact_price, filled_time, commission, slippage)
        self.portfolio_id = portfolio_id

    @classmethod
    def from_request(cls, request):
        """
        Generate new trade from request

        Args:
            request(dict): request database
        """
        return cls(**request)

    @classmethod
    def from_query(cls, query_data):
        """
        Recover existed order from query database

        Args:
            query_data(dict): query database
        """
        return cls.from_request(query_data)

    def to_mongodb_item(self):
        """
        To mongodb item
        """
        return self.to_dict()
    
   
class DigitalCurrencyTrade(ValueObject):
    """
    Digital Currency Trade data.
    """
    __slots__ = [
        'account_id',
        'amount',
        'cost',
        'exchange',
        'exchange_account_id',
        'exchange_order_id',
        'exchange_trade_id',
        'fee',
        'fee_currency',
        'order_id',
        'price',
        'side',
        'symbol',
        'timestamp',
    ]

    def __init__(self, account_id=None, amount=None, cost=None,
                 exchange=None, exchange_account_id=None,
                 exchange_order_id=None, exchange_trade_id=None,
                 fee=None, fee_currency=None, order_id=None,
                 price=None, side=None, symbol=None, timestamp=None):
        self.account_id = account_id
        self.amount = amount
        self.cost = cost
        self.exchange = exchange
        self.exchange_account_id = exchange_account_id
        self.exchange_order_id = exchange_order_id
        self.exchange_trade_id = exchange_trade_id
        self.fee = fee
        self.fee_currency = fee_currency
        self.order_id = order_id
        self.price = price
        self.side = side
        self.symbol = symbol
        self.timestamp = timestamp

    def __setattr__(self, key, value):
        """
        Set attr.

        Args:
            key(string): key
            value(obj): value
        """
        type_map = {
            'account_id': str,
            'amount': float,
            'cost': float,
            'exchange': str,
            'exchange_account_id': str,
            'exchange_order_id': str,
            'exchange_trade_id': str,
            'fee': float,
            'fee_currency': str,
            'order_id': str,
            'price': float,
            'side': str,
            'symbol': str,
            'timestamp': int,
        }
        object.__setattr__(self, key, type_map[key](value) if value is not None else value)

    @classmethod
    def from_subscribe(cls, item):
        """
        Generate from exchange subscribe.
        """
        mapper = {
            'exchange_order_id': 'orderId',
            'exchange': 'exchange',
            'timestamp': 'timestamp',
            'price': 'price',
            'order_id': 'extOrdId',
            'fee_currency': 'feeCurrency',
            'cost': 'cost',
            'account_id': 'accountId',
            'exchange_account_id': 'exchangeAccountId',
            'fee': 'fee',
            'exchange_trade_id': 'tradeId',
            'symbol': 'symbol',
            'amount': 'amount',
            'side': 'side'
        }
        return cls(**{key: item[mapper[key]] for key in cls.__slots__})

