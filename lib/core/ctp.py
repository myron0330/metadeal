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


class AccountResponse(ValueObject):

    __slots__ = [
        'spec_product_position_profit_by_alg',
        'pre_fund_mortgage_out',
        'delivery_margin',
        'frozen_commission',
        'curr_margin',
        'interest_base',
        'position_profit',
        'currency_id',
        'broker_id',
        'frozen_margin',
        'pre_margin',
        'withdraw_quota',
        'fund_mortgage_in',
        'deposit',
        'mortgage',
        'spec_product_commission',
        'spec_product_margin',
        'pre_credit',
        'pre_mortgage',
        'commission',
        'biz_type',
        'spec_product_close_profit',
        'interest',
        'pre_balance',
        'fund_mortgage_out',
        'exchange_margin',
        'available',
        'account_id',
        'pre_fund_mortgage_in',
        'fund_mortgage_available',
        'spec_product_frozen_margin',
        'pre_deposit',
        'spec_product_frozen_commission',
        'close_profit',
        'reserve_balance',
        'exchange_delivery_margin',
        'trading_day',
        'cash_in',
        'frozen_cash',
        'spec_product_exchange_margin',
        'settlement_id',
        'credit',
        'mortgageable_fund',
        'withdraw',
        'spec_product_position_profit',
        'balance',
        'reserve',
    ]

    def __init__(self, spec_product_position_profit_by_alg=None, pre_fund_mortgage_out=None, delivery_margin=None, frozen_commission=None, curr_margin=None, interest_base=None, position_profit=None, currency_id=None, broker_id=None, frozen_margin=None, pre_margin=None, withdraw_quota=None, fund_mortgage_in=None, deposit=None, mortgage=None, spec_product_commission=None, spec_product_margin=None, pre_credit=None, pre_mortgage=None, commission=None, biz_type=None, spec_product_close_profit=None, interest=None, pre_balance=None, fund_mortgage_out=None, exchange_margin=None, available=None, account_id=None, pre_fund_mortgage_in=None, fund_mortgage_available=None, spec_product_frozen_margin=None, pre_deposit=None, spec_product_frozen_commission=None, close_profit=None, reserve_balance=None, exchange_delivery_margin=None, trading_day=None, cash_in=None, frozen_cash=None, spec_product_exchange_margin=None, settlement_id=None, credit=None, mortgageable_fund=None, withdraw=None, spec_product_position_profit=None, balance=None, reserve=None):
        self.spec_product_position_profit_by_alg = spec_product_position_profit_by_alg
        self.pre_fund_mortgage_out = pre_fund_mortgage_out
        self.delivery_margin = delivery_margin
        self.frozen_commission = frozen_commission
        self.curr_margin = curr_margin
        self.interest_base = interest_base
        self.position_profit = position_profit
        self.currency_id = currency_id
        self.broker_id = broker_id
        self.frozen_margin = frozen_margin
        self.pre_margin = pre_margin
        self.withdraw_quota = withdraw_quota
        self.fund_mortgage_in = fund_mortgage_in
        self.deposit = deposit
        self.mortgage = mortgage
        self.spec_product_commission = spec_product_commission
        self.spec_product_margin = spec_product_margin
        self.pre_credit = pre_credit
        self.pre_mortgage = pre_mortgage
        self.commission = commission
        self.biz_type = biz_type
        self.spec_product_close_profit = spec_product_close_profit
        self.interest = interest
        self.pre_balance = pre_balance
        self.fund_mortgage_out = fund_mortgage_out
        self.exchange_margin = exchange_margin
        self.available = available
        self.account_id = account_id
        self.pre_fund_mortgage_in = pre_fund_mortgage_in
        self.fund_mortgage_available = fund_mortgage_available
        self.spec_product_frozen_margin = spec_product_frozen_margin
        self.pre_deposit = pre_deposit
        self.spec_product_frozen_commission = spec_product_frozen_commission
        self.close_profit = close_profit
        self.reserve_balance = reserve_balance
        self.exchange_delivery_margin = exchange_delivery_margin
        self.trading_day = trading_day
        self.cash_in = cash_in
        self.frozen_cash = frozen_cash
        self.spec_product_exchange_margin = spec_product_exchange_margin
        self.settlement_id = settlement_id
        self.credit = credit
        self.mortgageable_fund = mortgageable_fund
        self.withdraw = withdraw
        self.spec_product_position_profit = spec_product_position_profit
        self.balance = balance
        self.reserve = reserve

    @classmethod
    def from_ctp(cls, item):
        """
        Generate from ctp.

        Args:
            item(dict): ctp tick data

        Returns:
            obj: Tick instance
        """
        underline_item = {
            hump_to_underline(key): value for key, value in item.iteritems()
        }
        return cls(**underline_item)


class TradeResponse(ValueObject):

    __slots__ = [
        'instrument_id',
        'trader_id',
        'trading_role',
        'sequence_no',
        'order_sys_id',
        'exchange_id',
        'broker_id',
        'broker_order_seq',
        'price_source',
        'hedge_flag',
        'user_id',
        'investor_id',
        'business_unit',
        'trade_time',
        'trade_date',
        'trade_source',
        'direction',
        'trade_type',
        'price',
        'participant_id',
        'order_local_id',
        'order_ref',
        'volume',
        'offset_flag',
        'trade_id',
        'client_id',
        'trading_day',
        'exchange_inst_id',
        'clearing_part_id',
        'settlement_id',
    ]

    def __init__(self, instrument_id=None, trader_id=None, trading_role=None, sequence_no=None, order_sys_id=None, exchange_id=None, broker_id=None, broker_order_seq=None, price_source=None, hedge_flag=None, user_id=None, investor_id=None, business_unit=None, trade_time=None, trade_date=None, trade_source=None, direction=None, trade_type=None, price=None, participant_id=None, order_local_id=None, order_ref=None, volume=None, offset_flag=None, trade_id=None, client_id=None, trading_day=None, exchange_inst_id=None, clearing_part_id=None, settlement_id=None):
        self.instrument_id = instrument_id
        self.trader_id = trader_id
        self.trading_role = trading_role
        self.sequence_no = sequence_no
        self.order_sys_id = order_sys_id
        self.exchange_id = exchange_id
        self.broker_id = broker_id
        self.broker_order_seq = broker_order_seq
        self.price_source = price_source
        self.hedge_flag = hedge_flag
        self.user_id = user_id
        self.investor_id = investor_id
        self.business_unit = business_unit
        self.trade_time = trade_time
        self.trade_date = trade_date
        self.trade_source = trade_source
        self.direction = direction
        self.trade_type = trade_type
        self.price = price
        self.participant_id = participant_id
        self.order_local_id = order_local_id
        self.order_ref = order_ref
        self.volume = volume
        self.offset_flag = offset_flag
        self.trade_id = trade_id
        self.client_id = client_id
        self.trading_day = trading_day
        self.exchange_inst_id = exchange_inst_id
        self.clearing_part_id = clearing_part_id
        self.settlement_id = settlement_id

    @classmethod
    def from_ctp(cls, item):
        """
        Generate from ctp.

        Args:
            item(dict): ctp tick data

        Returns:
            obj: Tick instance
        """
        underline_item = {
            hump_to_underline(key): value for key, value in item.iteritems()
        }
        return cls(**underline_item)


__all__ = [
    'Tick',
    'AccountResponse',
    'TradeResponse'
]
