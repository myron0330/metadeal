# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Context FILE
#   Author: Myron
# **********************************************************************************#
import re
from collections import deque
from . clock import Clock
from .. data.asset_service import AssetType


class Context(object):

    def __init__(self, clock, sim_params, strategy,
                 market_service=None, universe_service=None,
                 asset_service=None, calendar_service=None,
                 market_roller=None, account_manager=None):
        self.sim_params = sim_params
        self._strategy = strategy
        self.market_service = market_service
        self.universe_service = universe_service
        self.asset_service = asset_service
        self.calendar_service = calendar_service
        self.market_roller = market_roller
        self.daily_trigger_time = '09:30'
        self.ban_level = 'l0'
        self._initial_value = None
        self._time_stamp_deque = deque([], maxlen=2)
        self._refresh_timestamp = deque([], maxlen=2)
        self.clock = clock or Clock(sim_params.freq)
        self.major_benchmark = sim_params.major_benchmark
        self.account_manager = account_manager
        self.compatible_account = self.account_manager.compatible_account
        self.registered_accounts = self.account_manager.registered_accounts
        self.registered_accounts_params = self.account_manager.registered_accounts_params
        origin_variables = set(dir(self))
        try:
            self._strategy.initialize(self)
            current_variables = set(dir(self))
            self._user_defined_variables = list(current_variables - origin_variables)
        except Exception, e:
            if not re.findall(re.compile('initialize'), e.__str__()):
                raise Exception('Exception in "Context.__init__": {}!'.format(e))
        finally:
            pass

    # *************************** common attributes *******************************#
    @property
    def current_date(self):
        """当前回测日期"""
        return self.clock.current_date

    @current_date.setter
    def current_date(self, *args):
        raise Exception('Exception in "{}": {}'.format('Context.current_date',
                                                       'user must not modify context.current_date!'))

    @property
    def previous_date(self):
        """当前回测日期的前一交易日"""
        return self.clock.previous_date

    @previous_date.setter
    def previous_date(self, *args):
        raise Exception('Exception in "{}": {}'.format('Context.previous_date',
                                                       'user must not modify context.previous_date!'))

    @property
    def current_minute(self):
        """当前分钟线"""
        return self.clock.current_minute

    @current_minute.setter
    def current_minute(self, *args):
        raise Exception('Exception in "{}": {}'.format('context.current_minute',
                                                       'user must not modify context.current_minute!'))

    @property
    def now(self):
        return self.clock.now

    def get_account(self, account_name):
        """
        Get account by account name.
        """
        return self.account_manager.get_account(account_name)

    def history(self, symbol='all', attribute='closePrice', time_range=1, freq='d', style='sat', rtype='frame',
                f_adj=None, s_adj='pre_adj', **options):
        raise NotImplementedError

    def get_universe(self, asset_type=AssetType.DIGITAL_CURRENCY, exclude_halt=False, with_position=False):
        if isinstance(asset_type, basestring):
            if asset_type == 'all':
                view_asset_type = AssetType.ALL
            elif asset_type == 'continuous_future':
                view_asset_type = [AssetType.CONTINUOUS_FUTURES]
            elif asset_type == 'futures':
                view_asset_type = AssetType.FUTURES
            else:
                view_asset_type = asset_type.split(',')
        else:
            view_asset_type = asset_type

        universe = set()
        for asset_type in view_asset_type:
            if asset_type in [AssetType.CONTINUOUS_FUTURES, AssetType.DIGITAL_CURRENCY]:
                univ = self.asset_service.filter_symbols(
                    asset_type, self.universe_service.dynamic_universe_dict[self.current_date])
                if asset_type == AssetType.CONTINUOUS_FUTURES:
                    univ = \
                        set(filter(lambda x: self.asset_service.get_asset_info(x).list_date <= self.current_date, univ))
                    univ = \
                        set(filter(lambda x:
                                   not (self.asset_service.get_asset_info(x).last_date and
                                        self.asset_service.get_asset_info(x).last_date < self.current_date), univ))
                universe |= univ
            elif asset_type == AssetType.BASE_FUTURES:
                candidate_futures_asset = \
                    self.asset_service.filter_assets(
                        asset_type=asset_type, symbols=self.universe_service.dynamic_universe_dict[self.current_date])
                universe |= \
                    set([asset.symbol for asset in filter(lambda x: x.list_date <= self.current_date <= x.last_date,
                                                          candidate_futures_asset)])
        return sorted(list(universe))

    def order_book(self):
        """
        Return order book data.
        """
        raise NotImplementedError

    def current_price(self, symbol):
        """
        查询前一回测点的symbol收盘价格
        Args:
            symbol(str): stock or futures symbol

        Returns:
            (float): 日线前收价格或者分钟线前一分钟收盘价格

        """
        date = self.current_date
        if self.asset_service.get_asset_info(symbol, date) is None:
            raise ValueError('Exception in "{}": Symbol: {} is not included in AssetService.'.format(
                "Context.current_price", symbol))
        if self.sim_params.freq == 'm':
            current_price = self.market_roller.current_price(symbol, date, self.current_minute)
            if current_price:
                return current_price
        previous_price = self.market_roller.reference_price(self.previous_date)
        current_price = previous_price.get(symbol)
        return current_price
