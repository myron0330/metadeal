# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
import time
from datetime import datetime
from utils.dict import CompositeDict
from utils.decorator import singleton
from .. configs import logger
from .. data.database_api import load_futures_rt_minute_data


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
            item = minute_bar_list[-1]
            item.update({
                'secID': symbol
            })
            info[symbol] = minute_bar_list[-1]
    return info


@singleton
class MarketQuote(object):

    def __init__(self, clock=None, universe=None, bar_collection=None):
        self.clock = clock
        self.universe = universe
        self.bar_collection = bar_collection or dict()
        self._bar_version = -1

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
                    self._refresh_bar_collection()
                    cur_info = self.bar_collection[self._bar_version]
                    for future in self.universe:
                        if future in cur_info:
                            if cur_info[future]['barTime'] == last_minute:
                                yield cur_info[future]
                except:
                    import traceback
                    logger.error('Fetch failed: %s' % (traceback.format_exc()))
            time.sleep(5)

    def publish_bar_data_section(self):
        """
        Publish the latest surface bar data.
        """
        last_minute = None
        while True:
            current_minute = self.clock.current_minute
            if last_minute != current_minute:
                last_minute = current_minute
                try:
                    second = datetime.now().second
                    if second < 2:
                        time.sleep(2 - second)
                    self._refresh_bar_collection()
                    current_info = self.bar_collection[self._bar_version]

                    response = CompositeDict()
                    for symbol in self.universe:
                        if symbol in current_info and current_info[symbol]['barTime'] == last_minute:
                            response[last_minute][symbol] = current_info[symbol]
                    if response:
                        yield response
                except:
                    import traceback
                    logger.error('publish bar data failed: %s' % (traceback.format_exc()))
            time.sleep(5)

    def _bar_version_next(self):
        """
        Bar version next.
        """
        return (self._bar_version + 1) % 2

    def _refresh_bar_collection(self):
        """
        Refresh bar collection.
        """
        self.bar_collection[self._bar_version_next()] = _get_minute_price_info(self.universe)
        self._bar_version = self._bar_version_next()
