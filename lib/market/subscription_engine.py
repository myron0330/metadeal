# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
from web.market.market import *


class MarketType(object):

    GDAX_TICK = 'GDAX_TICK'
    GDAX_ORDER_BOOK = 'GDAX_ORDER_BOOK'


market_type_map = {
    'GDAX_TICK': GDAXTicker,
    'GDAX_ORDER_BOOK': GDAXLevel
}


class SubscriptionEngine(object):

    @classmethod
    def fetch_market_quote(cls, market_type):
        """
        Fetch market data base on a specific market type

        Args:
            market_type(string): market type string
        """
        market_quote = market_type_map[market_type]()
        for data in market_quote.fetch_data():
            yield data

    @classmethod
    def fetch_trade_response(cls):
        """
        Fetch trade response
        """
        raise NotImplementedError
