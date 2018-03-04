# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
# todo. ADD market quote.


class MarketType(object):

    CTP_TICK = 'CTP_TICK'
    CTP_ORDER_BOOK = 'CTP_ORDER_BOOK'


market_type_map = {
    'CTP_TICK': (lambda x: x),
    'CTP_ORDER_BOOK': (lambda x: x)
}


class MarketEngine(object):

    @classmethod
    def fetch_data(cls, market_type):
        """
        Fetch data base on a specific market type

        Args:
            market_type(string): market type string
        """
        market_quote = market_type_map[market_type]()
        for data in market_quote.fetch_data():
            yield data
