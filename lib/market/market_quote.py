# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
import time
from datetime import datetime
from utils.decorator_utils import singleton
from .. configs import logger
from .. database.database_api import load_futures_rt_minute_data


class MarketType(object):

    CTP_TICK = 'CTP_TICK'
    CTP_ORDER_BOOK = 'CTP_ORDER_BOOK'


market_type_map = {
    'CTP_TICK': (lambda x: x),
    'CTP_ORDER_BOOK': (lambda x: x)
}


def _get_minute_price_info(universe):
    """
    Get the latest info of stocks.

    Args:
        universe(list): universe list
    """
    futures_rt_minute_data = load_futures_rt_minute_data(universe)
    info = dict()
    for symbol, minute_bar_list in futures_rt_minute_data.iteritems():
        if minute_bar_list:
            info[symbol] = minute_bar_list[-1]
    return info


@singleton
class MarketQuote(object):

    def __init__(self, clock=None, universe=None, tick_collection=None, bar_collection=None):
        self.clock = clock
        self.universe = universe
        self.tick_collection = tick_collection
        self.bar_collection = bar_collection or dict()
        self._bar_version = -1

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

    def bar_version_next(self):
        """
        Bar version next.
        """
        return (self._bar_version + 1) % 2

    def fetch_data_from_database_api(self):
        """
        Fetch data from database api.
        """
        last_minute = None
        while True:
            cur_minute = self.clock.current_minute
            if last_minute != cur_minute:
                last_minute = cur_minute
                try:
                    second = datetime.now().second
                    if second < 10:
                        time.sleep(10 - second)
                    self._refresh_future_data()
                    cur_info = self.bar_collection[self._bar_version]
                    for future in self.universe:
                        if future in cur_info:
                            if cur_info[future]['barTime'] == last_minute:
                                yield cur_info[future]
                except:
                    import traceback
                    logger.error('Fetch failed: %s' % (traceback.format_exc()))
            time.sleep(5)

    def _refresh_future_data(self):
        """
        Refresh future data
        """
        self.bar_collection[self.bar_version_next()] = _get_minute_price_info(self.universe)
        self._bar_version = self.bar_version_next()
