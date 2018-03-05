# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Live trading agent.
# **********************************************************************************#
import time
import json
from threading import Thread
from . context.parameters import SimulationParameters
from . const import DIGITAL_CURRENCY_PATTERN
from . market.market_base import *
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
        self._load_thread_pool(with_tick_channel=True,
                               with_order_book_channel=False,
                               with_response_channel=True,
                               with_clock_channel=True)

    def _load_thread_pool(self,
                          with_tick_channel=True,
                          with_order_book_channel=True,
                          with_response_channel=True,
                          with_clock_channel=True):
        """
        Load thread pool
        """
        full_universe = self.data_portal.universe_service.full_universe
        exchange_list = set({symbol.split('.')[-1] for symbol in full_universe
                             if DIGITAL_CURRENCY_PATTERN.match(symbol)})
        for exchange in exchange_list:
            if with_tick_channel:
                tick_channel = '{}_TICK'.format(exchange)
                self._thread_pool[tick_channel] = \
                    Thread(target=self._tick_engine, args=(tick_channel,))
            if with_order_book_channel:
                order_bool_channel = '{}_ORDER_BOOK'.format(exchange)
                self._thread_pool[order_bool_channel] = \
                    Thread(target=self._order_book_engine, args=(order_bool_channel,))
        if with_response_channel:
            # todo. complete the order response and trade response channel.
            self._thread_pool['RESPONSE'] = Thread(target=self._response_engine, )
        if with_clock_channel:
            self._thread_pool['CLOCK'] = Thread(target=self._handle_data_engine)

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
            item = json.loads(tick['data'])
            item.update({'channel': tick['channel']})
            tick_data = TickData.from_quote(item)
            parameters = {
                'strategy': self.strategy,
                'context': self.context,
                'tick': tick_data
            }
            self.trading_gateway.publish(EventType.event_on_tick, **parameters)

    def _order_book_engine(self, market_type):
        """
        Deal with on response event release.
        """
        for order_book in self.market_engine.fetch_data(market_type):
            item = json.loads(order_book['data'])
            item.update({'channel': order_book['channel']})
            order_book = OrderBookData.from_quote(item)
            parameters = {
                'strategy': self.strategy,
                'context': self.context,
                'order_book': order_book
            }
            self.trading_gateway.publish(EventType.event_order_book, **parameters)

    def _handle_data_engine(self, slots=0.5):
        """
        Handle data engine.
        """
        while True:
            time.sleep(slots)
            parameters = {
                'strategy': self.strategy,
                'context': self.context
            }
            self.trading_gateway.publish(EventType.event_handle_data, **parameters)

    def _response_engine(self):
        """
        Deal with on response event release.
        """
        raise NotImplementedError
