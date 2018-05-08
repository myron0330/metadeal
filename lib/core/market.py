# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
from utils.code_utils import (
    hump_to_underline
)
from . objects import ValueObject


class Tick(ValueObject):

    __slots__ = [
        'bid_price3',
        'bid_price2',
        'bid_price1',
        'pre_delta',
        'bid_price5',
        'bid_price4',
        'pre_close_price',
        'lower_limit_price',
        'upper_limit_price',
        'exchange_id',
        'pre_settlement_price',
        'curr_delta',
        'pre_open_interest',
        'open_interest',
        'update_millisecond',
        'ask_volume5',
        'ask_volume4',
        'ask_volume3',
        'ask_volume2',
        'ask_volume1',
        'action_day',
        'lowest_price',
        'highest_price',
        'bid_volume5',
        'bid_volume4',
        'update_time',
        'bid_volume1',
        'bid_volume3',
        'bid_volume2',
        'last_price',
        'ask_price1',
        'ask_price3',
        'ask_price2',
        'open_price',
        'ask_price4',
        'close_price',
        'trading_day',
        'volume',
        'average_price',
        'settlement_price',
        'ask_price5',
        'instrument_id',
        'exchange_inst_id',
        'turnover',
    ]

    def __init__(self, bid_price3=None, bid_price2=None, bid_price1=None, pre_delta=None,
                 bid_price5=None, bid_price4=None, pre_close_price=None, lower_limit_price=None,
                 upper_limit_price=None, exchange_id=None, pre_settlement_price=None,
                 curr_delta=None, pre_open_interest=None, open_interest=None,
                 update_millisecond=None, ask_volume5=None, ask_volume4=None,
                 ask_volume3=None, ask_volume2=None, ask_volume1=None,
                 action_day=None, lowest_price=None, highest_price=None,
                 bid_volume5=None, bid_volume4=None, update_time=None, bid_volume1=None,
                 bid_volume3=None, bid_volume2=None, last_price=None, ask_price1=None,
                 ask_price3=None, ask_price2=None, open_price=None, ask_price4=None,
                 close_price=None, trading_day=None, volume=None, average_price=None,
                 settlement_price=None, ask_price5=None, instrument_id=None,
                 exchange_inst_id=None, turnover=None):
        self.bid_price3 = bid_price3
        self.bid_price2 = bid_price2
        self.bid_price1 = bid_price1
        self.pre_delta = pre_delta
        self.bid_price5 = bid_price5
        self.bid_price4 = bid_price4
        self.pre_close_price = pre_close_price
        self.lower_limit_price = lower_limit_price
        self.upper_limit_price = upper_limit_price
        self.exchange_id = exchange_id
        self.pre_settlement_price = pre_settlement_price
        self.curr_delta = curr_delta
        self.pre_open_interest = pre_open_interest
        self.open_interest = open_interest
        self.update_millisecond = update_millisecond
        self.ask_volume5 = ask_volume5
        self.ask_volume4 = ask_volume4
        self.ask_volume3 = ask_volume3
        self.ask_volume2 = ask_volume2
        self.ask_volume1 = ask_volume1
        self.action_day = action_day
        self.lowest_price = lowest_price
        self.highest_price = highest_price
        self.bid_volume5 = bid_volume5
        self.bid_volume4 = bid_volume4
        self.update_time = update_time
        self.bid_volume1 = bid_volume1
        self.bid_volume3 = bid_volume3
        self.bid_volume2 = bid_volume2
        self.last_price = last_price
        self.ask_price1 = ask_price1
        self.ask_price3 = ask_price3
        self.ask_price2 = ask_price2
        self.open_price = open_price
        self.ask_price4 = ask_price4
        self.close_price = close_price
        self.trading_day = trading_day
        self.volume = volume
        self.average_price = average_price
        self.settlement_price = settlement_price
        self.ask_price5 = ask_price5
        self.instrument_id = instrument_id
        self.exchange_inst_id = exchange_inst_id
        self.turnover = turnover

    @classmethod
    def from_ctp(cls, item):
        """
        Generate from ctp.

        Args:
            item(dict): ctp tick data

        Returns:
            obj: Tick instance
        """
        item['update_millisecond'] = item.pop('UpdateMillisec')
        underline_item = {
            hump_to_underline(key): value for key, value in item.iteritems()
        }
        return cls(**underline_item)


__all__ = [
    'Tick'
]
