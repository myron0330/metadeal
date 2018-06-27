# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
from . ctpGateway import CTPMarketGateway, CtpTraderGateway


class CTPGateway(object):

    def __init__(self, user_id=None, password=None, broker_id=None, market_address=None,
                 trader_address=None, event_engine=None,
                 market_gateway=None, trader_gateway=None):
        self.user_id = user_id
        self.password = password
        self.broker_id = broker_id
        self.market_address = market_address
        self.trader_address = trader_address
        self.event_engine = event_engine
        self.market_gateway = market_gateway
        self.trader_gateway = trader_gateway

    @classmethod
    def from_config(cls, ctp_config, event_engine=None):
        """
        Generate from config.

        Args:
            ctp_config(dict): ctp account config dict
            event_engine(obj): event engine

        Returns:
            obj: CTPGateway
        """
        user_id = ctp_config['user_id']
        password = ctp_config['password']
        broker_id = ctp_config['broker_id']
        market_address = ctp_config['market_address']
        trader_address = ctp_config['trader_address']
        market_gateway = CTPMarketGateway(
            user_id=user_id, password=password, broker_id=broker_id,
            address=market_address, event_engine=event_engine
        )
        trader_gateway = CtpTraderGateway(
            user_id=user_id, password=password, broker_id=broker_id,
            address=trader_address, event_engine=event_engine
        )
        ctp_config['market_gateway'] = market_gateway
        ctp_config['trader_gateway'] = trader_gateway
        ctp_config['event_engine'] = event_engine
        return cls(**ctp_config)

    def prepare_initialize(self, universe=None):
        """
        Prepare market gateway and trader gateway.

        Args:
            universe(list): symbol list
        """
        self.market_gateway.connect()
        if universe:
            self.market_gateway.subscribe(universe)
        self.trader_gateway.connect()

    def query_information(self):
        """
        Query information from ctp.
        """
        self.trader_gateway.query_account()
        self.trader_gateway.query_positions()
