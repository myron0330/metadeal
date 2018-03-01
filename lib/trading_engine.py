# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Live trading agent.
# **********************************************************************************#
import time
import json
from threading import Thread
from . context.parameters import SimulationParameters
from . const import DIGITAL_CURRENCY_PATTERN
from . market.market_base import TickData
from . event.event_base import EventType


class TradingEngine(object):

    def __init__(self, clock, sim_params,
                 strategy, data_portal,
                 context, account_manager,
                 market_engine=None,
                 trading_gateway=None):
        assert isinstance(sim_params, SimulationParameters)
        self.clock = clock
        self.sim_params = sim_params
        self.strategy = strategy
        self.data_portal = data_portal
        self.context = context
        self.account_manager = account_manager
        self.market_engine = market_engine
        self.trading_gateway = trading_gateway
        self._thread_pool = dict()

    def initialize(self):
        """
        Prepare initialize.
        """
        self._load_thread_pool()

    def _load_thread_pool(self):
        """
        Load thread pool
        """
        full_universe = self.data_portal.universe_service.full_universe
        exchange_list = set({symbol.split('.')[-1] for symbol in full_universe
                             if DIGITAL_CURRENCY_PATTERN.match(symbol)})
        for exchange in exchange_list:
            tick_channel = '{}_TICK'.format(exchange)
            self._thread_pool[tick_channel] = \
                Thread(target=self._tick_engine, args=(tick_channel,))

    def start(self):
        """
        LiveTradingAgent worker start.
        """
        self.trading_gateway.start()
        for _, thread in self._thread_pool.iteritems():
            thread.start()

    def stop(self):
        """
        LiveTradingAgent worker stop.
        """
        self.trading_gateway.stop()
        for _, thread in self._thread_pool.iteritems():
            thread.join()

    def _tick_engine(self, market_type):
        """
        Deal with on tick event release.
        """
        for tick in self.market_engine.fetch_data(market_type):
            tick_data = TickData.from_quote(json.loads(tick['database']))
            kwargs = {
                'strategy': self.strategy,
                'context': self.context,
                'tick': tick_data
            }
            self.trading_gateway.publish(EventType.event_on_tick, **kwargs)

    @staticmethod
    def response_engine(self):
        """
        Deal with on response event release.
        """
        counter = 0
        while True:
            time.sleep(1)
            print 'on_response_worker', counter
            counter += 1

    def _order_book_engine(self, market_type):
        """
        Deal with on response event release.
        """
        counter = 0
        while True:
            time.sleep(1)
            print 'on_order_book_worker', market_type, counter
            counter += 1
