# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
from unittest import TestCase
from lib.market.market_quote import MarketQuote
from lib.core.clock import Clock


class TestMarketQuote(TestCase):

    def setUp(self):
        self.clock = Clock()
        self.market_quote = MarketQuote(clock=self.clock)

    def test_fetch_from_api(self):
        """
        Test fetch from api.
        """
        self.market_quote.universe = ['RB1810', 'RM809']
        for data in self.market_quote.fetch_data_from_database_api():
            print data
        pass
