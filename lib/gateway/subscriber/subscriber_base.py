# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Market base file.
# **********************************************************************************#
import requests
import logging
import requests.packages.urllib3
from configs import (
    position_url,
    order_url,
    trade_url
)
from quartz.core.objects import ValueObject
logging.getLogger("requests").setLevel(logging.WARNING)


class TickData(ValueObject):
    """
    Tick data
    """
    __slots__ = [
        'best_ask',
        'best_bid',
        'high_24h',
        'last_size',
        'low_24h',
        'open_24h',
        'price',
        'symbol',
        'sequence',
        'side',
        'time',
        'receive_timestamp',
        'trade_id',
        'volume_24h',
        'volume_30d',
        'channel',
    ]

    def __init__(self,
                 best_ask=None, best_bid=None, high_24h=None,
                 last_size=None, low_24h=None, open_24h=None,
                 price=None, symbol=None, sequence=None,
                 side=None, time=None, receive_timestamp=None,
                 trade_id=None, volume_24h=None, volume_30d=None,
                 channel=None):
        self.best_ask = best_ask
        self.best_bid = best_bid
        self.high_24h = high_24h
        self.last_size = last_size
        self.low_24h = low_24h
        self.open_24h = open_24h
        self.price = price
        self.symbol = symbol
        self.receive_timestamp = receive_timestamp
        self.sequence = sequence
        self.side = side
        self.time = time
        self.trade_id = trade_id
        self.volume_24h = volume_24h
        self.volume_30d = volume_30d
        self.channel = channel

    def __setattr__(self, key, value):
        """
        Set attr.

        Args:
            key(string): key
            value(obj): value
        """
        type_map = {
            'best_ask': float,
            'best_bid': float,
            'high_24h': float,
            'last_size': float,
            'low_24h': float,
            'open_24h': float,
            'price': float,
            'symbol': str,
            'sequence': int,
            'side': str,
            'time': str,
            'receive_timestamp': int,
            'trade_id': int,
            'volume_24h': float,
            'volume_30d': float,
            'channel': str,
        }
        object.__setattr__(self, key, type_map[key](value) if value is not None else value)

    @classmethod
    def from_subscribe(cls, item):
        """
        Generate from market subscribe.
        """
        item['receive_timestamp'] = item.pop('receiveTimestamp')
        item['symbol'] = item.pop('product_id')
        item.pop('type')
        return cls(**item)


class OrderBookData(ValueObject):
    """
    Order book data
    """
    __slots__ = [
        'changes',
        'symbol',
        'receive_timestamp',
        'time',
        'channel'
    ]

    def __init__(self, changes=None, symbol=None, receive_timestamp=None, time=None, channel=None):
        self.changes = changes
        self.symbol = symbol
        self.receive_timestamp = receive_timestamp
        self.time = time
        self.channel = channel

    @classmethod
    def from_subscribe(cls, item):
        """
        Generate from market subscribe.
        """
        item['receive_timestamp'] = item.pop('receiveTimestamp')
        item['symbol'] = item.pop('product_id')
        item.pop('type')
        return cls(**item)


class TradeData(ValueObject):
    """
    Trade data.
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
            'accountId': 'account_id',
            'amount': 'amount',
            'cost': 'cost',
            'exchange': 'exchange',
            'exchangeAccountId': 'exchange_account_id',
            'orderId': 'exchange_order_id',
            'tradeId': 'exchange_trade_id',
            'fee': 'fee',
            'feeCurrency': 'fee_currency',
            'extOrdId': 'order_id',
            'price': 'price',
            'side': 'side',
            'symbol': 'symbol',
            'timestamp': 'timestamp',
        }
        return cls(**{key: item[mapper[key]] for key in cls.__slots__})


def fetch_position_from_pms(account_id):
    """
    Fetch position from pms.

    Args:
        account_id(string): account id
    """
    params = {'accountId': account_id}
    rsp = requests.get(position_url, params=params)
    return rsp.status_code, rsp.json()


def fetch_trade_from_pms(account_id, order_id):
    """
    Fetch trade from pms.

    Args:
        account_id(string): account id
        order_id(string): order id
    """
    params = {'accountId': account_id, "extOrdId": order_id}
    rsp = requests.get(trade_url, params=params)
    return rsp.status_code, rsp.json()


def fetch_order_from_pms(account_id, order_id):
    """
    Fetch order from pms.

    Args:
        account_id(string): account id
        order_id(string): order id
    """
    params = {'accountId': account_id, "extOrdId": order_id}
    rsp = requests.get(order_url, params=params)
    return rsp.status_code, rsp.json()


__all__ = [
    'TickData',
    'OrderBookData',
    'TradeData',
    'fetch_position_from_pms',
    'fetch_order_from_pms',
    'fetch_trade_from_pms'
]
