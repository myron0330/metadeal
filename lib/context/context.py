# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Context FILE
#   Author: Myron
# **********************************************************************************#
import re
import pandas as pd
import numpy as np
from copy import copy
from collections import deque
from datetime import datetime, timedelta
from .. api import api_method, set_account_instance
from .. data.asset_service import AssetType, StockAssetInfo, FuturesAssetInfo,OTCFundAssetInfo,IndexAssetInfo
# from .. data.market_service import tas_data_tick_expand
# from .. data.tick_roller import TickRoller
from .. data.signal import signalgenerator
from .. utils.factor_utils import INTERNAL_FIELDS, DATABASE_SIGNALS_NAME
from .. account import StockAccount, FuturesAccount, OTCFundAccount, IndexAccount
from .. utils.datetime_utils import get_trading_days
from .. utils.error_utils import *
from .. const import EQUITY_DAILY_FIELDS, EQUITY_MINUTE_FIELDS


def compatible_wrapper(func):
    """
    Decorator: Compatible to Quartz 2
    Args:
        func(func): function

    Returns:
        obj: func output
    """
    def _wrapper(self, *args, **kwargs):
        if not self.compatible_account:
            raise BacktestError('Method "{}" is unsupported in Quartz 3, '
                                'please check the user documents for instruction.'.format(func.__name__))
        return func(self, *args, **kwargs)
    return _wrapper


MULTI_FREQUENCY_PATTERN = re.compile('(\d*)(d|m)')


class Clock(object):

    previous_date = None
    current_date = None
    current_minute = None
    previous_minute = None

    def __init__(self, freq):
        self.freq = freq

    def update_time(self, previous_date=None, current_date=None, minute=None):
        """
        Update time

        Args:
            previous_date (datetime): previous date
            current_date (datetime): current date
            minute (str): minute
        Returns:
             datetime: current timestamp
        """
        self.previous_date = previous_date if previous_date else self.previous_date
        self.current_date = current_date if current_date else self.current_date
        self.previous_minute = self.current_minute if minute else self.previous_minute
        self.current_minute = minute if minute else self.current_minute

    @property
    def now(self):
        """
        Returns:
             datetime: current timestamp
        """
        if self.current_minute:
            hour = int(self.current_minute.split(':')[0])
            minute = int(self.current_minute.split(':')[1])
        else:
            hour, minute = 0, 0
        second = 0
        return self.with_(hour=hour, minute=minute, second=second)

    @staticmethod
    def format_time(date, hour, minute, second):
        """
        Format specific timestamp

        Args:
            date (datetime): date
            hour (int): hours
            minute (int): minutes
            second (int): seconds

        Returns:
            datetime: the specific timestamp
        """
        seconds = 3600 * hour + 60 * minute + second
        return date + timedelta(seconds=seconds)

    def with_(self, hour=None, minute=None, second=None, previous_date=False):
        """
        Format specific timestamp

        Args:
            hour (int): hours
            minute (int): minutes
            second (int): seconds
            previous_date (boolean): whether to use previous date or not

        Returns:
            datetime: the specific timestamp
        """
        date = self.current_date if not previous_date else self.previous_date
        return datetime(date.year, date.month, date.day, hour or date.hour,
                        minute or date.minute, second or self.current_date.second)

    def __repr__(self):
        """
        Returns:
             datetime: current timestamp
        """
        if self.freq == 'd':
            return self.current_date.strftime('%Y-%m-%d')
        else:
            return self.now.strftime('%Y-%m-%d %H:%M')


class Context(object):

    def __init__(self, clock, sim_params, strategy,
                 market_service=None, universe_service=None,
                 asset_service=None, calendar_service=None, market_roller=None,
                 account_manager=None):
        set_account_instance(self)
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
        # todo: registered_accounts in both context and account_manager, refactor!
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
            import quartz
            quartz.data.signal.signal.name2id = {}

    def signal_flag(self):
        return hasattr(self, '_signal_generator')

    @property
    @compatible_wrapper
    def pending_blotter(self):
        """
        当前未成交订单委托
        """
        if self.clock.freq == 'd':
            raise ValueError('There is no "pending_blotter" in daily backtest!')
        return self.compatible_account.broker.blotter.pending_blotters()

    # *************************** public methods *******************************#
    @property
    def initial_value(self):
        """
        初始的权益，用于计算用户收益曲线
        """
        if self._initial_value is None:
            initial_position_value = 0
            for account_name, account_config in self.sim_params.accounts.iteritems():
                initial_position_value += \
                    sum([position * account_config.cost_base.get(symbol, 0)
                         for symbol, position in account_config.position_base.iteritems()])
            initial_capital_base = \
                sum([account.cash for account in self.registered_accounts.itervalues()])
            self._initial_value = initial_capital_base + initial_position_value
        return self._initial_value

    # def update_info(self, minute_data=None, all_minute_bars=None):
    #     """
    #     更新行情信息
    #     """
    #     self.all_minute_bars = all_minute_bars or self.all_minute_bars
    #     self.transact_minute_tick_data = minute_data or self.transact_minute_tick_data

    # def pre_trading_day(self):
    #     """
    #     开盘前处理
    #     """
    #     for account_name, account in self.registered_accounts.iteritems():
    #         account.broker.opening_check()
    #
    # def calculate_reference_information(self):
    #     """
    #     同步当前信息，包括最新价、浮动盈亏等
    #     """
    #     for account_name, account in self.registered_accounts.iteritems():
    #         account.broker.calculate_reference_information()
    #
    # def handle_data(self):
    #     """
    #     处理用户策略
    #     """
    #     self._strategy.handle_data(self)
    #
    # def transact(self):
    #     """
    #     撮合交易
    #     """
    #     for account_name, account in self.registered_accounts.iteritems():
    #         account.broker.transact()
    #
    # def settlement(self):
    #     """
    #     每日结算
    #     """
    #     for account_name, account in self.registered_accounts.iteritems():
    #         account.broker.settlement()
    #
    # def post_trading_day(self):
    #     """
    #     用户盘后处理逻辑
    #     """
    #     self._strategy.post_trading_day(self)
    #
    # def broker_post_trading_day(self):
    #     """
    #     Broker 每天盘后处理逻辑
    #     """
    #     for account_name, account in self.registered_accounts.iteritems():
    #         account.broker.post_trading_day()
    #
    # def publish_orders(self):
    #     """
    #     Publish orders
    #     """
    #     trading_orders = dict()
    #     self.calculate_reference_information()
    #     self.handle_data()
    #     for account_name, account in self.registered_accounts.iteritems():
    #         trading_orders[account_name] = account.get_orders()
    #     return trading_orders

    @compatible_wrapper
    def to_record(self):
        """
        生成用于回测记录的状态数据
        Return:
            dict, str: object: 用于输出的一些变量，键为变量名，值为变量当前的值
        """
        return self.compatible_account.to_record(major_benchmark=self.major_benchmark)

    # ******************************************************************************#
    #   The following items are used for user interfaces
    # ******************************************************************************#
    # *************************** common attributes *******************************#
    @property
    @compatible_wrapper
    def universe(self):
        return self.get_universe(asset_type=AssetType.TRADABLE, exclude_halt=True, with_position=True)

    @universe.setter
    def universe(self, *args):
        raise Exception('Exception in "{}": {}'.format('Context.universe',
                                                       'user must not modify context.universe!'))

    @property
    @compatible_wrapper
    def current_universe(self):
        return list(self.universe_service.view(self.current_date, remove_halt=False))

    @current_universe.setter
    def current_universe(self, *args):
        raise Exception('Exception in "{}": {}'.format('Context.current_universe',
                                                       'user must not modify context.current_universe!'))

    @property
    def cash(self):
        """当前仓位中的现金"""
        aggregated_cash = 0.
        for account_name, account in self.registered_accounts.iteritems():
            aggregated_cash += account.cash
        return aggregated_cash

    @cash.setter
    def cash(self, *args):
        raise Exception('Exception in "{}": {}'.format('Context.cash',
                                                       'user must not modify context.cash!'))

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
    @compatible_wrapper
    def blotter(self):
        """当前指令簿"""
        return self.compatible_account.blotter

    @blotter.setter
    def blotter(self, *args):
        raise Exception('Exception in "{}": {}'.format('Context.blotter',
                                                       'user must not modify context.blotter!'))

    @property
    @compatible_wrapper
    def position(self):
        return self.compatible_account.position

    @position.setter
    def position(self, *args):
        raise Exception('Exception in "{}": {}'.format('Context.position',
                                                       'user must not modify context.position!'))

    # ***************************** stocks attributes ****************************#
    @property
    @compatible_wrapper
    def security_position(self):
        """当前仓位中的证券头寸"""
        return self.compatible_account.security_position

    @security_position.setter
    def security_position(self, *args):
        raise Exception('Exception in "{}": {}'.format('Context.security_position',
                                                       'user must not modify context.security_position!'))

    @property
    @compatible_wrapper
    def security_cost(self):
        """当前仓位中的证券成本"""
        portfolio = self.compatible_account.broker.portfolio
        return portfolio.seccost

    @security_cost.setter
    def security_cost(self, *args):
        raise Exception('Exception in "{}": {}'.format('Context.security_cost',
                                                       'user must not modify context.security_cost!'))

    @property
    @compatible_wrapper
    def avail_security_position(self):
        """当前仓位中的可卖证券仓位"""
        return self.compatible_account.available_security_position

    @avail_security_position.setter
    def avail_security_position(self, *args):
        raise Exception('Exception in "{}": {}'.format('Context.avail_security_position',
                                                       'user must not modify context.avail_security_position!'))

    @property
    @compatible_wrapper
    def reference_price(self):
        """证券参考价格"""
        if self.clock.freq == 'd':
            return self.market_roller.reference_price(self.previous_date)
        else:
            return self.market_roller.reference_price(self.current_date, self.current_minute)

    @reference_price.setter
    def reference_price(self, *args):
        raise Exception('Exception in "{}": {}'.format('Context.reference_price',
                                                       'user must not modify context.reference_price!'))

    @property
    @compatible_wrapper
    def reference_return(self):
        """证券参考收益率"""
        if self.clock.freq == 'd':
            return self.market_roller.reference_return(self.current_date)
        else:
            return self.market_roller.reference_return(self.current_date, self.current_minute)

    @reference_return.setter
    def reference_return(self, *args):
        raise Exception('Exception in "{}": {}'.format('Context.reference_return',
                                                       'user must not modify context.reference_return!'))

    @property
    @compatible_wrapper
    def reference_portfolio_value(self):
        """账户参考总体价值"""
        return self.compatible_account.portfolio_value

    @reference_portfolio_value.setter
    def reference_portfolio_value(self, *args):
        raise Exception('Exception in "{}": {}'.format('Context.reference_portfolio_value', 'User must not modify context.reference_portfolio_value!'))

    @property
    def referencePrice(self):
        message = "Warning: account.referencePrice is deprecated, please use account.reference_price instead."
        WARNER.warn(message)
        return self.reference_price

    @referencePrice.setter
    def referencePrice(self, *args):
        raise Exception('Exception in "{}": {}'.format('Context.referencePrice',
                                                       'user must not modify context.referencePrice!'))

    @property
    def referenceReturn(self):
        message = "Warning: account.referenceReturn is deprecated, please use account.reference_return instead."
        WARNER.warn(message)
        return self.reference_return

    @referenceReturn.setter
    def referenceReturn(self, *args):
        raise Exception('Exception in "{}": {}'.format('Context.referenceReturn',
                                                       'user must not modify context.referenceReturn!'))

    @property
    def referencePortfolioValue(self):
        message = "Warning: account.referencePortfolioValue is deprecated, " \
                  "please use account.reference_portfolio_value instead."
        WARNER.warn(message)
        return self.reference_portfolio_value

    @referencePortfolioValue.setter
    def referencePortfolioValue(self, *args):
        raise Exception('Exception in "{}": {}'.format('Context.referencePortfolioValue',
                                                       'user must not modify context.referencePortfolioValue!'))

    @property
    @compatible_wrapper
    def avail_secpos(self):
        message = "Warning: account.avail_secpos is deprecated, please use account.avail_security_position instead."
        WARNER.warn(message)
        return self.compatible_account.available_security_position

    @avail_secpos.setter
    def avail_secpos(self, *args):
        raise Exception('Exception in "{}": {}'.format('Context.avail_secpos',
                                                       'user must not modify context.avail_secpos!'))
    @property
    @compatible_wrapper
    def valid_secpos(self):
        message = "Warning: account.valid_secpos is deprecated, please use account.security_position instead."
        WARNER.warn(message)
        return self.compatible_account.security_position

    @valid_secpos.setter
    def valid_secpos(self, *args):
        raise Exception('Exception in "{}": {}'.format('Context.valid_secpos',
                                                       'user must not modify context.valid_secpos!'))

    @property
    @compatible_wrapper
    def secpos(self):
        return self.compatible_account.security_position

    @secpos.setter
    def secpos(self, *args):
        raise Exception('Exception in "{}": {}'.format('Context.secpos',
                                                       'user must not modify context.secpos!'))

    @property
    def valid_seccost(self):
        message = "Warning: account.valid_seccost is deprecated, please use account.security_cost instead."
        WARNER.warn(message)
        return self.security_cost

    @valid_seccost.setter
    def valid_seccost(self, *args):
        raise Exception('Exception in "{}": {}'.format('Context.valid_seccost',
                                                       'user must not modify context.valid_seccost!'))

    # *************************** signal methods *******************************#
    def compute_signals(self, trading_days):
        """
        计算信号数值
        Dependency:
            account: Account类的实例，对于QuartzData需要用到其中signal_generator相关信息
        Args:
            trading_days (list of datetime): 交易日列表
        """
        self._signal_generator.raw_factor_data = self.market_service.stock_market_data.daily_bars
        self.signal_generator.signal_computation(self, trading_days)

    @property
    def signal_generator(self):
        """
        signal_generator 属性
        """
        return self._signal_generator

    @signal_generator.setter
    def signal_generator(self, signal_gen):
        """
        注册signal

        Args:
            signal_gen (SignalGenerator): 信号生成器，代码在data/signalgenerator
        """
        if not isinstance(signal_gen, signalgenerator.SignalGenerator):
            raise TypeError('Exception in "{}": {}'.format('Context.signal_generator',
                                                           'context.signal_generator should be an '
                                                           'instance of SignalGenerator!'))
        else:
            self._signal_generator = signal_gen
            self.sim_params.max_history_window_daily += signal_gen.max_window_length

    @api_method
    def get_account(self, account_name):
        """
        获取账户
        """
        return self.account_manager.get_account(account_name)

    # *************************** order methods *******************************#
    @compatible_wrapper
    @api_method
    def order(self, symbol, amount, price=0., order_type='market', **kwargs):
        """
        下单 (兼容 Quartz 2.x)
        """
        return self.compatible_account.order(symbol, amount, price=price, order_type=order_type, **kwargs)

    @compatible_wrapper
    @api_method
    def order_to(self, symbol, amount, price=0., order_type='market', **kwargs):
        """
        在handle_data(account, data)中使用，向account.blotter属性添加Order实例；
        指令含义为买入（卖出）一定量的证券使得证券symbol交易后的数量为amount

        Args:
            symbol (str): 需要交易的证券代码，必须包含后缀，其中上证证券为.XSHG，深证证券为.XSHE
            amount (float or int): 需要交易的证券代码为symbol的证券数量，为正则为买入，为负则为卖出；
                                    程序会自动对amount向下取整到最近的整百
            price (float): 交易指令限价（仅分钟线策略可用）
            order_type (str): 交易指令类型，可以为'market'或'limit'，默认为'market'，为limit时仅日内策略可用
        """
        return self.compatible_account.order_to(symbol, amount, price=price, order_type=order_type, **kwargs)

    @compatible_wrapper
    @api_method
    def order_pct(self, symbol, pct, price=0., order_type='market', **kwargs):
        """
        在handle_data(account, data)中使用，向account.blotter属性添加Order实例；指令含义为买入（卖出）价值为虚拟账户当前总价值
        的pct百分比的的证券symbol，仅限市价单

        Args:
            symbol (str): 需要交易的证券代码，必须包含后缀，其中上证证券为.XSHG，深证证券为.XSHdf
            pct (float or int): 需要交易的证券代码为symbol的证券占虚拟账户当前总价值的百分比，范围为 -1 ~ 1，为正则为买入，
            为负则为卖出；程序会自动对amount向下取整到最近的整百
            price (float): 交易指令限价（仅分钟线策略可用）
            order_type (str): 交易指令类型，可以为'market'或'limit'，默认为'market'，为limit时仅日内策略可用
        """
        return self.compatible_account.order_pct(symbol, pct, price=price, order_type=order_type)

    @compatible_wrapper
    @api_method
    def order_pct_to(self, symbol, pct, price=0., order_type='market', **kwargs):
        """
        在handle_data(account, data)中使用，向account.blotter属性添加Order实例；指令含义为买入（卖出）
        证券symbol使得其价值为虚拟账户当前总价值的pct百分比，仅限市价单

        Args:
            symbol (str): 需要交易的证券代码，必须包含后缀，其中上证证券为.XSHG，深证证券为.XSHE
            pct (float or int): 需要交易的证券代码为symbol的证券占虚拟账户当前总价值的百分比，范围为 -1 ~ 1，
            为正则为买入，为负则为卖出；程序会自动对amount向下取整到最近的整百
            price (float): 交易指令限价（仅分钟线策略可用）
            order_type (str): 交易指令类型，可以为'market'或'limit'，默认为'market'，为limit时仅日内策略可用
        """
        return self.compatible_account.order_pct_to(symbol, pct, price=price, order_type=order_type, **kwargs)

    @compatible_wrapper
    @api_method
    def cancel_order(self, order_id):
        """
        在handle_data(account)中使用，从Account实例中的account.blotter属性中撤回对应order_id的指令，
        表现为该order的state属性变为"OrderStatus.Cancelled"，并不再进行成交。

        Args:
            order_id: 需要撤回的订单id

        Returns:
            boolean: 订单是否取消成功
        """
        return self.compatible_account.cancel_order(order_id)

    @compatible_wrapper
    @api_method
    def get_order(self, order_id):
        """
        通过order_id获取最新订单对象，并可通过order_id.state获取最新订单状态

        Args:
            order_id: str，每次order完成之后会返回的order_id

        Returns:
            Order or None: order_id满足查询要求的Order对象，如果id不存在，则返回None
        """
        return self.compatible_account.get_order(order_id)

    @compatible_wrapper
    @api_method
    def get_orders(self, status=None, symbol='all'):
        """
        通过订单状态查询当前满足要求的订单，当前支持的订单状态可详见文档。
        Args:
            status: OrderStatus，不同订单状态可详见文档
            symbol: str or list，订单所属证券或证券列表，可使用'all'表示所有订单
        Returns:
            list: 满足state的Order对象列表
        """
        return self.compatible_account.get_orders(status=status, symbol=symbol)

    def _check_time_range(self, time_range, freq):
        if not isinstance(time_range, int) or time_range <= 0:
            # raise ValueError('Exception in "{}": {}'.format('Context.history',
            #                                                 'time_range must be an positive integer less than or equal '
            #                                                 'your current max daily history window '
            #                                                 '({})!'.format(self.sim_params.max_history_window_daily)))
            msg = InternalCheckMessage.HISTORY_TIME_RANGE_INPUT_ERROR.format(time_range)
            raise InternalCheckError(msg)
        if freq == 'd' and time_range > self.sim_params.max_history_window_daily:
            # raise ValueError('Exception in "{}": {}'.format('Context.history',
            #                                                 'history overflow. Your current max daily history window '
            #                                                 'is {}. Please use a shorter parameter, or change '
            #                                                 'max_history_window in your initialize()!'.format(
            #                                                     self.sim_params.max_history_window_daily)))
            msg = InternalCheckMessage.HISTORY_TIME_RANGE_MAX_ERROR.format(time_range)
            raise InternalCheckError(msg)
        elif freq == 'm':
            if self.sim_params.freq == 'd':
                raise ValueError('Exception in "{}": {}'.format('Context.history',
                                                                'minute history can be queried only '
                                                                'when backtest parameter freq=\'m\''))
            if time_range > self.sim_params.max_history_window_minute:
                # raise ValueError('Exception in "{}": {}'.format('Context.history',
                #                                                 'History overflow. Your current '
                #                                                 'max intraday history window is {}. '
                #                                                 'Please use a shorter parameter, or change '
                #                                                 'max_history_window in your initialize()!'.format(
                #                                                     self.sim_params.max_history_window_minute)))
                msg = InternalCheckMessage.HISTORY_TIME_RANGE_MAX_ERROR.format(time_range)
                raise InternalCheckError(msg)

    def timestamp_last_refresh(self):
        if len(self._refresh_timestamp) == 2:
            return self._refresh_timestamp[0]
        else:
            return None

    def update_refresh_timestamp(self):
        timestamp = (self.current_date, self.current_minute)
        if not self._refresh_timestamp:
            self._refresh_timestamp.append(timestamp)
        if self._refresh_timestamp[-1] != timestamp:
            self._refresh_timestamp.append(timestamp)

    def _valid_symbols(self, symbol):
        valid_symbols = self.universe_service.full_universe
        if symbol is None or symbol == 'all':
            return list(valid_symbols)
        elif isinstance(symbol, list):
            invalid_symbols = set(symbol) - set(valid_symbols)
            if len(invalid_symbols) != 0:
                # raise ValueError('[{}] are not valid query symbols!'.format(','.join(invalid_symbols)))
                msg = InternalCheckMessage.SYMBOL_ERROR.format(' '.join(symbol))
                raise InternalCheckError(msg)
            return symbol
        elif symbol in valid_symbols:
            return [symbol]
        else:
            # raise ValueError('{} is not a valid query symbols!'.format(symbol))
            msg = InternalCheckMessage.SYMBOL_ERROR.format(symbol)
            raise InternalCheckError(msg)

    def _valid_attributes(self, attribute, freq):
        attribute = [attribute] if isinstance(attribute, basestring) else attribute
        if freq in 'md':
            # valid_attributes = self.market_service.available_fields(freq=freq)
            valid_attributes = DATABASE_SIGNALS_NAME
        else:
            raise ValueError('Exception in "{}": {}'.format('Context.history',
                                                            'freq must be \'d\'(daily) or \'m\'(minute)! '))
        if attribute is None or attribute == 'default':
            return ['closePrice', 'openPrice', 'highPrice', 'lowPrice']
        elif isinstance(attribute, list):
            invalid_attributes = set(attribute) - set(valid_attributes)
            if len(invalid_attributes) != 0:
                # raise ValueError('Exception in "{}": {}'.format('Context.history',
                #                                                 'attribute can only be among {}. Please verify the '
                #                                                 'attribute!'.format(valid_attributes)))
                msg = InternalCheckMessage.MARKET_DATA_ATTRIBUTE_ERROR.format(' '.join(attribute), valid_attributes)
                raise InternalCheckError(msg)
            non_internal_factors = set(attribute) - set(INTERNAL_FIELDS)
            if freq == 'm' and len(non_internal_factors) > 0:
                raise ValueError('Exception in "{}": {}'.format('Context.history',
                                                                'Stock factor data is always daily!'))
            return attribute
        elif attribute in valid_attributes:
            if freq == 'm' and attribute in INTERNAL_FIELDS:
                raise ValueError('Exception in "{}": {}'.format('Context.history',
                                                                'Stock factor data is always daily!'))
            return [attribute]
        else:
            # raise ValueError('Exception in "{}": {}'.format('Context.history',
            #                                                 'attribute can only be among {}. Please verify the '
            #                                                 'attribute!'.format(valid_attributes)))
            msg = InternalCheckMessage.MARKET_DATA_ATTRIBUTE_ERROR.format(attribute, valid_attributes)
            raise InternalCheckError(msg)

    def _valid_prepare_dates(self, current_date, time_range, ratio):
        from math import ceil
        step = int(ceil(time_range * ratio / 240.0))
        trading_days_list = self.calendar_service.get_direct_trading_day_list(current_date, step, False)
        return trading_days_list

    @staticmethod
    def _valid_freq(freq):
        assert isinstance(freq, basestring), 'Freq must be in type of string! '
        match = MULTI_FREQUENCY_PATTERN.match(freq)
        if match is None:
            raise AttributeError('freq must be string that starts with interval number and ends with \'m\' or \'d\'')
        interval = int(match.group(1)) if len(match.group(1)) > 0 else 1
        frequency = match.group(2)
        if frequency == 'd' and interval != 1:
            raise AttributeError('daily history do not support multi-frequency freq')
        if frequency == 'm' and interval not in (1, 5, 15, 30, 60):
            raise AttributeError('minute history only support multi-frequency with freq in '
                                 '(\'1m\', \'5m\', \'15m\', \'30m\', \'60m\')')
        return interval, frequency

    @property
    def now(self):
        return self.clock.now

    # *************************** history methods *******************************#
    @api_method
    def history(self, symbol='all', attribute='closePrice', time_range=1, freq='d', style='sat', rtype='frame',
                f_adj=None, s_adj='pre_adj', **options):
        if rtype not in ['frame', 'array']:
            msg = InternalCheckMessage.RTYPE_ERROR.format(rtype)
            raise InternalCheckError(msg)
        interval_freq = freq
        ratio, freq = self._valid_freq(freq)
        self._check_time_range(time_range, freq)
        symbols = self._valid_symbols(symbol)
        attributes = self._valid_attributes(attribute, freq)
        if freq == 'd':
            date = self.current_date if self.clock.current_minute == '17:00' else self.previous_date
            return self.market_service.slice(symbols=symbols, fields=attributes, time_range=time_range,
                                             end_date=date, freq=freq, style=style, rtype=rtype,
                                             f_adj=f_adj, s_adj=s_adj)
        elif freq == 'm':
            if len(self.asset_service.filter_symbols(AssetType.FUTURES, symbols)) != 0 and style is not 'sat':
                raise AttributeError('style must be \'sat\' when history symbol contains futures')
            current_minute_bars = self.market_service.minute_bar_map[self.current_date.strftime('%Y-%m-%d')]
            minute = current_minute_bars[-1] if self.clock.current_minute == '17:00' else self.clock.current_minute
            end_minute = self.calendar_service.get_trade_time(self.current_date, minute)
            prepare_dates = self._valid_prepare_dates(self.current_date, time_range, ratio)
            prepare_dates = prepare_dates if len(prepare_dates) > 2 else [self.previous_date, self.current_date]
            return self.market_roller.slice(prepare_dates, end_minute, fields=attributes, time_range=time_range,
                                            symbols=symbols, style=style, rtype=rtype, freq=interval_freq)
        raise AttributeError('Unknown freq: {} type, freq and only be \'d\' or \'m\''.format(freq))

    @api_method
    def get_symbol_history(self, symbol, time_range=10, freq=None, f_adj=None):
        """获取单只证券的历史数据，日线和分钟线默认调用的方法不同，参数分别见相应方法"""
        freq = self.sim_params.freq if freq is None else freq
        field = EQUITY_DAILY_FIELDS if freq == 'd' else EQUITY_MINUTE_FIELDS
        if symbol == 'tradeDate':
            symbol = self.sim_params.benchmarks[0]
            hist = self.history(symbol=symbol, attribute='closePrice', time_range=time_range,
                                freq=freq, style='sat', rtype='frame', f_adj=f_adj)[symbol]['closePrice']
            return np.array([t[:10] for t in hist.index])
        elif symbol == 'minuteBar' and freq == 'm':
            symbol = self.sim_params.benchmarks[0]
            hist = self.history(symbol=symbol, attribute='closePrice', time_range=time_range,
                                freq=freq, style='sat', rtype='frame', f_adj=f_adj)[symbol]['closePrice']
            return np.array(hist.index)
        elif symbol == 'benchmark':
            symbol = self.sim_params.benchmarks[0]
            pre_close_price = self.history(symbol=symbol, attribute='preClosePrice', time_range=time_range, freq='d',
                                           style='sat', rtype='array', f_adj=f_adj)[symbol]['preClosePrice']
            hist = self.history(symbol=symbol, attribute=field, time_range=time_range, freq=freq,
                                style='sat', rtype='array', f_adj=f_adj)[symbol]
            hist['return'] = hist['closePrice'] / pre_close_price - 1.0
            return hist
        else:
            return self.history(symbol=symbol, attribute=field, time_range=time_range, freq=freq,
                                style='sat', rtype='array', f_adj=f_adj)[symbol]

    @compatible_wrapper
    def get_attribute_history(self, attribute, time_range, freq=None, f_adj=None):
        """获取单个数据变量的历史数据，日线和分钟线默认调用的方法不同，参数分别见相应方法"""
        freq = self.sim_params.freq if freq is None else freq
        equities = list(self.asset_service.filter_symbols(
            asset_type=AssetType.EQUITIES, symbols=self.universe_service.full_universe))
        hist = copy(self.history(symbol=equities, attribute=attribute, time_range=time_range,
                                 freq=freq, style='ast', rtype='array', f_adj=f_adj)[attribute])
        hist.pop('time', None)
        return hist

    @compatible_wrapper
    def get_history(self, time_range, freq=None, f_adj=None):
        """获取所有历史数据，日线和分钟线默认调用的方法不同，参数分别见相应方法"""
        equities = list(self.asset_service.filter_symbols(asset_type=AssetType.EQUITIES,
                                                          symbols=self.universe_service.full_universe))
        freq = self.sim_params.freq if freq is None else freq
        if freq == 'd':
            hist = self.history(symbol=equities, attribute=EQUITY_DAILY_FIELDS, time_range=time_range, freq='d',
                                style='sat', rtype='array', f_adj=f_adj)
        elif freq == 'm':
            hist = self.history(symbol=equities, attribute=EQUITY_MINUTE_FIELDS,
                                time_range=time_range, freq='m', style='sat', rtype='array', f_adj=f_adj)
        else:
            raise ValueError('Exception in "{}": {}'.format('Context.get_history',
                                                            'freq must be \'d\'(daily) or \'m\'(minute)! '))
        hist['tradeDate'] = self.calendar_service.within_interval(self.previous_date, time_range)
        hist['benchmark'] = hist[self.sim_params.benchmarks[0]]
        return hist

    @compatible_wrapper
    def get_daily_symbol_history(self, symbol, time_range, f_adj=None):
        """获取单只证券的日线历史数据，参数见相应方法"""
        return self.get_symbol_history(symbol, time_range, freq='d', f_adj=f_adj)

    @compatible_wrapper
    def get_daily_attribute_history(self, attribute, time_range, f_adj=None):
        """获取单个数据变量的日线历史数据，参数见相应方法"""
        return self.get_attribute_history(attribute, time_range, freq='d', f_adj=f_adj)

    @compatible_wrapper
    def get_daily_history(self, time_range, f_adj=None):
        """获取所有日线历史数据，参数见相应方法"""
        return self.get_history(time_range, freq='d', f_adj=f_adj)

    # *************************** other methods ******************************* #
    @compatible_wrapper
    @api_method
    def observe(self, name, value):
        """
        在handle_data(account, data)中使用，仅限日间策略。
        在backtest函数的输出的DataFrame（默认变量为bt）中增加一列自定义需要观测的变量。
        Args:
            name (str): 需要观测的变量名称
            value (object): 需要观测的变量值
        """
        return self.compatible_account.observe(name, value)

    @api_method
    def get_universe(self, asset_type=AssetType.ALL, exclude_halt=False, with_position=False):
        if isinstance(asset_type, basestring):
            if asset_type == 'all':
                view_asset_type = AssetType.ALL
            elif asset_type == 'continuous_future':
                # COMPATIBLE
                view_asset_type = [AssetType.CONTINUOUS_FUTURES]
            elif asset_type == 'futures':
                view_asset_type = AssetType.FUTURES
            elif asset_type == 'equities':
                view_asset_type = AssetType.EQUITIES
            elif asset_type == 'fund':
                view_asset_type = AssetType.FUND
            elif asset_type == 'tradeable':
                view_asset_type = AssetType.TRADABLE
            else:
                view_asset_type = asset_type.split(',')
        else:
            view_asset_type = asset_type

        universe = set()
        for asset_type in view_asset_type:
            if asset_type == AssetType.STOCK:
                if with_position:
                    security_accounts = self.account_manager.filter_accounts(account_type='security')
                    security_positions = [set(account.security_position) for account in security_accounts.itervalues()]
                    security_positions = reduce(lambda x, y: x | y, security_positions)
                    universe |= self.asset_service.filter_symbols(
                        symbols=list(self.universe_service.view(self.current_date, remove_halt=exclude_halt,
                                                                position_securities=security_positions)),
                        asset_type=asset_type)
                else:
                    universe |= self.asset_service.filter_symbols(
                        symbols=list(self.universe_service.view(self.current_date, remove_halt=exclude_halt)),
                        asset_type=asset_type)
            elif asset_type in [AssetType.INDEX, AssetType.EXCHANGE_FUND, AssetType.FUND,
                                AssetType.OTC_FUND, AssetType.CONTINUOUS_FUTURES]:
                univ = self.asset_service.filter_symbols(asset_type, self.universe_service.dynamic_universe_dict[
                    self.current_date])
                if asset_type == AssetType.CONTINUOUS_FUTURES:
                    univ = set(filter(lambda x: self.asset_service.get_asset_info(x).list_date <= self.current_date,
                                      univ))
                    univ = set(filter(lambda x: not (self.asset_service.get_asset_info(x)._last_date and
                                           self.asset_service.get_asset_info(x)._last_date < self.current_date), univ))
                universe |= univ
            elif asset_type == AssetType.BASE_FUTURES:
                candidate_futures_asset = self.asset_service.filter_assets(asset_type=asset_type,
                                                                           symbols=
                                                                           self.universe_service.dynamic_universe_dict[
                                                                               self.current_date])
                universe |= \
                    set([asset.symbol for asset in filter(lambda x: x.list_date <= self.current_date <= x._last_date,
                                                          candidate_futures_asset)])
        return sorted(list(universe))

    def get_symbol(self, symbols):
        """
        将期货代码列表（可含具体合约及连续合约），解析成context当前下的普通合约代码结果
        Args:
            symbols(list of str): 连续合约代码列表

        Returns:
            str or list of str: 单个普通合约或者普通合约代码列表

        """
        current_date = self.clock.current_date.strftime('%Y%m%d')
        if isinstance(symbols, (str, unicode)) and not symbols.count(','):
            return self._to_base_futures(symbols, current_date)
        symbols = symbols.split(',') if isinstance(symbols, basestring) else symbols
        ordinary_symbols = filter(lambda x: len(x) > 4, symbols)
        continuous_symbols = filter(lambda x: len(x) <= 4, symbols)
        continuous_symbols = list(set(continuous_symbols))
        if continuous_symbols:
            transferred_symbols = map(lambda x: self._to_base_futures(x, current_date), continuous_symbols)
            transferred_symbols = filter(lambda x: x is not None, transferred_symbols)
            return list(set(transferred_symbols + ordinary_symbols))
        else:
            return list(set(ordinary_symbols))

    def _to_base_futures(self, continuous_symbol=None, trade_date=None):
        """
        获取连续合约代码在trade_date所对应的普通合约代码
        Args:
            continuous_symbol(str): 连续合约代码
            trade_date(str): 日期，格式如 %Y%m%d or %Y-%m-%d

        Returns:
            str: 普通合约代码

        """
        if not continuous_symbol or len(continuous_symbol) > 4:
            return continuous_symbol
        t_date = pd.to_datetime(trade_date)
        date_uncovered = t_date < self.asset_service.artificial_begin_date or \
                         t_date > self.asset_service.artificial_end_date
        if self.asset_service.get_asset_info(continuous_symbol) is None or date_uncovered:
            self.asset_service.update_with_asset_symbols(
                continuous_futures=[continuous_symbol], start_date=t_date, end_date=t_date)
        transferred_symbol = self.asset_service.get_asset_info(continuous_symbol).get_symbol(t_date)
        if transferred_symbol is None:
            raise DataLoadError("读取期货合约Artificial信息错误。")
        else:
            return transferred_symbol

    def transfer_cash(self, origin, target, amount):
        """
        账户间转帐

        Args:
            origin(str or Account object): 待转出账户名或账户对象
            target(str or Account object): 待转入账户名或账户对象
            amount(int or float): 转帐金额
        """
        origin_account = origin \
            if isinstance(origin, (StockAccount, FuturesAccount, OTCFundAccount)) else self.get_account(origin)
        target_account = target \
            if isinstance(target, (StockAccount, FuturesAccount, OTCFundAccount)) else self.get_account(target)
        if amount > origin_account.cash:
            raise BacktestInputError('Amount must be less than the available cash of origin account! '
                                     ''.format(origin_account.cash))
        origin_account.change_cash(origin_account.cash - amount)
        target_account.change_cash(target_account.cash + amount)
        for account_name, account in self.registered_accounts.iteritems():
            account.broker.calculate_reference_information()

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
        # freq == 'd' or 当天还没有行情更新
        previous_price = self.market_roller.reference_price(self.previous_date)
        current_price = previous_price.get(symbol)
        # if not current_price:
        return current_price

    # def _tick_data_past(self, symbol):
    #     """
    #     查询分钟线回测时当天已出现的行情中，symbol的最新close_price
    #     Args:
    #         symbol（str): 股票或期货symbol
    #
    #     Returns:
    #         (float), 成交价格
    #
    #     """
    #     if symbol in self.transact_minute_tick_data.get(self.clock.current_minute):
    #         return self.transact_minute_tick_data.get(self.clock.current_minute).get(symbol)
    #     t_date = self.clock.current_date.strftime("%Y-%m-%d")
    #     all_minute_before = [e for e in self.market_service.minute_bar_map[t_date][::-1]
    #                          if (e < self.clock.current_minute < '15:16') or ('20:50' < e < self.clock.current_minute)]
    #     for time in all_minute_before:
    #         if symbol in self.transact_minute_tick_data.get(time):
    #             return self.transact_minute_tick_data.get(time).get(symbol)
    #     return None

    def _get_user_variable(self):
        result = []
        for var in self._user_defined_variables:
            if var not in ['daily_trigger_time', 'signal_generator', 'compatible_account', 'universe_service']:
                result.append(var)
        return result

    def loads_variables_from_dump_data(self, dump_data):
        import cPickle
        if dump_data is None:
            return
        for key, value in dump_data.iteritems():
            try:
                setattr(self, key, cPickle.loads(str(value)))
            except Exception, e:
                print 'context.{} can not be load from dumped file'.format(key)

    def dumps_variables_as_dump_data(self):
        import cPickle
        dump_data = {}
        for key in self._get_user_variable():
            try:
                dump_data[key] = cPickle.dumps(getattr(self, key))
            except Exception, e:
                print 'context.{} can not be dumped'.format(key)
        return dump_data

    def get_rolling_tuple(self, symbol=None):
        """
        返回连续合约symbol, 在回测期间所对应的具体合约切换信息。无切换时返回交易日所对应具体合约。

        Args:
            symbol(str): 人工合约代码，如'IFM0':

        Returns:
            (tuple), 如('IF1701', 'IF1702')

        """
        if symbol is None or isinstance(symbol, list):
            raise BacktestInputError('Please input artificial future symbol format as: IFM0')
        if self.timestamp_last_refresh() is None or self.timestamp_last_refresh()[0] == self.current_date:
            mapping_pass_days = [self.current_date]
        else:
            last_refresh_date = self.timestamp_last_refresh()[0]
            mapping_pass_days = get_trading_days(last_refresh_date, self.current_date)
            mapping_pass_days.remove(last_refresh_date)
        mapping_begin, mapping_end = None, None
        for d in mapping_pass_days:
            mapping_day = self.asset_service._artificial_switch_info.get(d)
            # 对于mapping_day为{}，或者mapping_day.get(symbol)取不到，都继续循环
            try:
                symbol_before, symbol_after = mapping_day.get(symbol)
                if mapping_begin is None:
                    mapping_begin = symbol_before
                mapping_end = symbol_after
            except:
                continue
        # 如果refresh期间无人工合约切换，symbol_before为期间第一天的symbol_transfer
        if mapping_end is None:
            mapping_begin = self.get_symbol(symbol)
            mapping_end = mapping_begin
        return mapping_begin, mapping_end

    def mapping_changed(self, symbol=None):
        """
        判断symbol是否存在人工合约切换
        Parameters
        ----------
        symbol(str): 必输入项，人工合约如'IFM0'

        Returns(boolean): True of False
        -------

        """
        if symbol is None or isinstance(symbol, list):
            raise BacktestInputError('Please input artificial future symbol format as: IFM0')
        if self.timestamp_last_refresh() is None or self.timestamp_last_refresh()[0] == self.current_date:
            mapping_pass_days = [self.current_date]
        else:
            last_refresh_date = self.timestamp_last_refresh()[0]
            mapping_pass_days = get_trading_days(last_refresh_date, self.current_date)
            mapping_pass_days.remove(last_refresh_date)
        switch_infos = self.asset_service._artificial_switch_info[mapping_pass_days]
        result = any(e.get(symbol, None) for e in switch_infos.values)
        return result
