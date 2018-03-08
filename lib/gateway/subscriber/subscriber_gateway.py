# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
from web.market.market import *
from . position_subscriber import PositionSubscriber
from . trade_subscriber import TradesSubscriber


class MarketType(object):

    GDAX_TICK = 'GDAX_TICK'
    GDAX_ORDER_BOOK = 'GDAX_ORDER_BOOK'


market_type_map = {
    'GDAX_TICK': GDAXTicker,
    'GDAX_ORDER_BOOK': GDAXLevel
}


class SubscriberGateway(object):

    def __init__(self, account_ids=None,
                 position_subscriber_pool=None,
                 trade_subscriber_pool=None):
        """
        Args:
            account_ids(list): account ids
            position_subscriber_pool(dict): account_id --> position_subscriber
            trade_subscriber_pool(dict): account_id --> trade_subscriber
        """
        self.account_ids = account_ids
        self.position_subscriber_pool = position_subscriber_pool
        self.trade_subscriber_pool = trade_subscriber_pool

    @classmethod
    def from_config(cls, sim_params=None, **kwargs):
        """
        Generate from parameters and web cache.

        Args:
            sim_params(obj): sim parameters
        """
        account_ids = {config.account_id for _, config in sim_params.accounts.iteritems()}
        position_subscriber_pool = dict()
        trade_subscriber_pool = dict()
        for account_id in account_ids:
            position_subscriber_pool[account_id] = PositionSubscriber(account_id)
            trade_subscriber_pool[account_id] = TradesSubscriber(account_id)
        return cls(account_ids=sim_params,
                   position_subscriber_pool=position_subscriber_pool,
                   trade_subscriber_pool=trade_subscriber_pool)

    @classmethod
    def fetch_market_quote(cls, market_type):
        """
        Fetch market data base on a specific market type.

        Args:
            market_type(string): market type string
        """
        market_quote = market_type_map[market_type]()
        for data in market_quote.fetch_data():
            yield data

    def query_position_detail(self, account_id):
        """
        Query position detail of account id.

        Args:
            account_id(string): account id
        """
        return self.position_subscriber_pool[account_id].fetch_data()

    def fetch_trade_response(self):
        """
        Fetch trade response.
        """
        raise NotImplementedError
