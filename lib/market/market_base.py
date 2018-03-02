# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Market base file.
# **********************************************************************************#
from .. core.objects import ValueObject


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

    @classmethod
    def from_quote(cls, item):
        """
        Generate from market quote.
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
    def from_quote(cls, item):
        """
        Generate from market quote.
        """
        item['receive_timestamp'] = item.pop('receiveTimestamp')
        item['symbol'] = item.pop('product_id')
        item.pop('type')
        return cls(**item)


__all__ = [
    'TickData',
    'OrderBookData'
]
