# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Subscriber gateway.
# **********************************************************************************#
from lib.event.event_base import EventType
from .. base_gateway import BaseSubscriberGateway


class MarketType(object):

    FUTURES = 'futures'


class SubscriberGateway(BaseSubscriberGateway):

    def __init__(self, account_ids=None,
                 position_subscriber_pool=None,
                 trade_subscriber_pool=None,
                 event_engine=None):
        """
        Args:
            account_ids(list): account ids
            position_subscriber_pool(dict): account_id --> position_subscriber
            trade_subscriber_pool(dict): account_id --> trade_subscriber
            event_engine(obj): event engine
        """
        super(SubscriberGateway, self).__init__()
        self.account_ids = account_ids
        self.position_subscriber_pool = position_subscriber_pool
        self.trade_subscriber_pool = trade_subscriber_pool
        self.event_engine = event_engine

    @classmethod
    def from_config(cls, sim_params=None, event_engine=None, **kwargs):
        """
        Generate from parameters and web cache.

        Args:
            sim_params(obj): sim parameters
            event_engine(obj): event engine
        """
        raise NotImplementedError

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

    def subscribe_trade(self, trade_list):
        """
        Subscribe trade.

        Args:
            trade_list(list): trade list
        """
        for item in trade_list:
            trade = DigitalCurrencyTrade.from_subscribe(item)
            parameters = {
                'trade': trade
            }
            self.event_engine.publish(EventType.event_deal_with_trade, **parameters)
