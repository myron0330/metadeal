# -*- coding: utf-8 -*-
# **********************************************************************************#
#     File: StockAccount FILE
# **********************************************************************************#
import logging
import numpy as np
from copy import deepcopy
from . base_account import BaseAccount
from .. trade import Order
from .. utils.error_utils import (
    WARNER,
    Errors
)


def order_pre_process(func):
    """
    Decorator: Order pre process
    """
    def decorator(*args, **kwargs):
        if kwargs.get('otype'):
            kwargs['order_type'] = kwargs.get('otype')
        return func(*args, **kwargs)
    return decorator


class StockAccount(BaseAccount):
    """
    股票账户
    """
    def __init__(self, clock, data_portal, is_backtest=True):
        super(StockAccount, self).__init__(clock=clock,
                                           data_portal=data_portal,
                                           is_backtest=is_backtest)
        self.account_type = 'security'
        self.record = dict()

    @property
    def security_position(self):
        """股票持仓"""
        return {security: position.amount for security, position in self.get_positions().iteritems()}

    @property
    def available_security_position(self):
        """
        股票可用持仓
        """
        return {security: position.available_amount for security, position in self.get_positions().iteritems()}

    @order_pre_process
    def order(self, symbol, amount, price=0., order_type='market', algo_params=None, **kwargs):
        """
        在handle_data(account, data)中使用，向account.blotter属性添加Order实例；指令含义为买入（卖出）数量为amount的证券symbol

        Args:
            symbol (str): 需要交易的证券代码，必须包含后缀，其中上证证券为.XSHG，深证证券为.XSHE
            amount (float or int): 需要交易的证券代码为symbol的证券数量，为正则为买入，为负则为卖出；程序会自动对amount向下取整到最近的整百
            price (float): 交易指令限价（仅分钟线策略可用）
            order_type (str): 交易指令类型，可以为'market'或'limit'，默认为'market'，为limit时仅日内策略可用
            algo_params: 算法交易配置
        """
        if not isinstance(amount, (int, long, float)) or np.isnan(amount):
            raise Errors.INVALID_ORDER_AMOUNT
        amount = self.prepare_amount(amount)
        if amount == 0 or np.isinf(amount):
            return
        if self.clock.freq == 'm':
            timestamp = "{} {}".format(self.clock.current_date.strftime('%Y-%m-%d'), self.clock.current_minute)
            if order_type == 'limit':
                if isinstance(price, (int, long, float)) and price > 0:
                    order = Order(symbol, amount, timestamp, order_type, price, algo_params=algo_params)
                else:
                    WARNER.warn('WARNING: price {} is not supported for limit order, will use market order instead'
                                .format(price))
                    order = Order(symbol, amount, timestamp, algo_params=algo_params)
            else:
                order = Order(symbol, amount, timestamp, algo_params=algo_params)
        else:
            timestamp = "{} 09:30".format(self.clock.current_date.strftime('%Y-%m-%d'))
            if order_type == 'limit':
                message = 'WARNING: limit order is not supported in daily trade, will use market order instead.'
                WARNER.warn(message)
            order = Order(symbol, amount, timestamp, algo_params=algo_params)
        if order.algo_params is not None and self._is_backtest:
            message = 'WARNING: Order algo_params ignored in backtest, ' \
                      'this may work after strategy submitted to livetrading.'
            WARNER.warn(message)
        self.submitted_orders.append(order)
        return order.order_id

    @order_pre_process
    def order_to(self, symbol, amount, price=0., order_type='market', algo_params=None, **kwargs):
        """
        在handle_data(account, data)中使用，向account.blotter属性添加Order实例；指令含义为买入（卖出）一定量的证券使得证券symbol交易后的数量为amount

        Args:
            symbol (str): 需要交易的证券代码，必须包含后缀，其中上证证券为.XSHG，深证证券为.XSHE
            amount (float or int): 需要交易的证券代码为symbol的证券数量，为正则为买入，为负则为卖出；程序会自动对amount向下取整到最近的整百
            price (float): 交易指令限价（仅分钟线策略可用）
            order_type (str): 交易指令类型，可以为'market'或'limit'，默认为'market'，为limit时仅日内策略可用
            algo_params: 算法交易配置
        """
        if not isinstance(amount, (int, long, float)):
            raise ValueError('Order amount must be integer or float number!')
        if np.isnan(amount):
            raise ValueError('Order amount is nan!')
        elif amount < 0:
            raise ValueError('Order amount must be positive!')
        amount -= self.security_position.get(symbol, 0)
        amount = self.prepare_amount(amount)
        if amount == 0 or np.isinf(amount):
            return
        return self.order(symbol, amount, price, order_type, algo_params=algo_params)

    @order_pre_process
    def order_pct(self, symbol, pct, price=0., order_type='market', algo_params=None, **kwargs):
        """
        在handle_data(account, data)中使用，向account.blotter属性添加Order实例；指令含义为买入（卖出）价值为虚拟账户当前总价值的pct百分比的的证券symbol，仅限市价单

        Args:
            symbol (str): 需要交易的证券代码，必须包含后缀，其中上证证券为.XSHG，深证证券为.XSHE
            pct (float or int): 需要交易的证券代码为symbol的证券占虚拟账户当前总价值的百分比，范围为 -1 ~ 1，为正则为买入，为负则为卖出；程序会自动对amount向下取整到最近的整百
            price (float): 交易指令限价（仅分钟线策略可用）
            order_type (str): 交易指令类型，可以为'market'或'limit'，默认为'market'，为limit时仅日内策略可用
            algo_params: 算法交易配置
        """
        if not 0 <= pct <= 1:
            raise Errors.INVALID_ORDER_PCT_AMOUNT
        if order_type != 'market':
            raise Errors.INVALID_ORDER_PCT_TYPE

        p = self.get_reference_price(symbol)
        if np.isnan(p) or p == 0:
            logging.warn('No valid reference price of {} at {}! It might be a newly-issued security. '
                         'This order is ignored.'.format(symbol, self.clock.current_date.strftime("%Y-%m-%d")))
            return
        amount = self.portfolio_value * pct / p
        if np.isnan(amount):
            raise Errors.INVALID_ORDER_AMOUNT
        amount = self.prepare_amount(amount)
        if amount == 0 or np.isinf(amount):
            return
        return self.order(symbol, amount, price, order_type, algo_params=algo_params)

    @order_pre_process
    def order_pct_to(self, symbol, pct, price=0., order_type='market', algo_params=None, **kwargs):
        """
        在handle_data(account, data)中使用，向account.blotter属性添加Order实例；指令含义为买入（卖出）证券symbol使得其价值为虚拟账户当前总价值的pct百分比，仅限市价单

        Args:
            symbol (str): 需要交易的证券代码，必须包含后缀，其中上证证券为.XSHG，深证证券为.XSHE
            pct (float or int): 需要交易的证券代码为symbol的证券占虚拟账户当前总价值的百分比，范围为 -1 ~ 1，为正则为买入，为负则为卖出；程序会自动对amount向下取整到最近的整百
            price (float): 交易指令限价（仅分钟线策略可用）
            order_type (str): 交易指令类型，可以为'market'或'limit'，默认为'market'，为limit时仅日内策略可用
            algo_params: 算法交易配置
        """
        if not 0 <= pct <= 1:
            raise Errors.INVALID_ORDER_PCT_AMOUNT
        if order_type != 'market':
            raise Errors.INVALID_ORDER_PCT_TYPE

        p = self.get_reference_price(symbol)
        if np.isnan(p) or p == 0:
            logging.warn('No valid reference price of {} at {}! It might be a newly-issued security. '
                         'This order is ignored.'.format(symbol, self.clock.current_date.strftime("%Y-%m-%d")))
            return

        target_amount = self.portfolio_value * pct / p
        amount = target_amount - self.security_position.get(symbol, 0)
        if np.isnan(amount):
            raise Errors.INVALID_ORDER_AMOUNT
        amount = self.prepare_amount(amount)
        if amount == 0 or np.isinf(amount):
            return
        return self.order(symbol, amount, price, order_type, algo_params=algo_params)

    def get_position(self, symbol):
        """
        返回某只证券的持仓明细

        Args:
            symbol: 具体证券代码

        Returns:
            Position: 返回该证券代码所对应的 Position 对象; 若不包含该持仓，返回 None
        """
        return self.position.get(symbol, None)

    def get_positions(self, exclude_halt=False):
        """
        返回当前账户的持仓明细
        exclude_halt(Boolean): 是否去除停牌股票

        Returns:
            dict: 返回当前所有持仓，key 为证券代码，value 为对应的 Position 对象
        """
        if exclude_halt:
            current_positions = self.position
            inactive_stocks = \
                self.data_portal.universe_service.untradable_dict[self.clock.current_date]
            return {stock: position for stock, position in current_positions.iteritems()
                    if stock not in inactive_stocks}
        return self.position

    def get_trades(self):
        """
        返回当前账户的交易明细

        Returns:
            list: 当日 trades 列表
        """
        # todo. adapt trades to account.
        raise NotImplementedError

    def close_all_positions(self, symbol=None):
        """
        对股票进行全部平仓

        Args:
            symbol(basestring or list): Optional, 具体证券代码或证券代码列表
        """
        symbols = symbol if symbol is not None else self.get_positions().keys()
        symbols = symbols if isinstance(symbols, list) else [symbols]
        for stock in symbols:
            self.order_to(stock, 0)

    @staticmethod
    def prepare_amount(amount):
        """
        Normalize amount according to buy sell.
        Args:
            amount(int): stock amount

        Returns:
            int: amount
        """
        if np.isinf(amount):
            return amount
        if amount > 0:
            amount = int(amount) / 100 * 100
        elif amount < 0:
            amount = int(amount)
        return amount

    def get_reference_price(self, symbol):
        """
        Get reference price.
        Args:
            symbol(string): symbol

        Returns:
            float: price
        """
        # todo: 应该用未复权的价格进行order_pct_to
        previous_date = self.clock.previous_date.strftime('%Y-%m-%d')
        reference_data = self.data_portal.application_data.adj_reference_price
        return reference_data[previous_date].get(symbol, 0)

    def observe(self, name, value):
        """
        在handle_data(context, data)中使用，仅限日间策略。在回测输出的DataFrame（默认为bt）中增加一列自定义需要观测的变量。

        Args:
            name (str): 需要观测的变量名称
            value (object): 需要观测的变量值
        """
        # todo. realizing observe function in a better way.
        if self.clock.freq != 'd':
            logging.warn('Warning: observe function is only supported by daily backtest!')
            return

        if name in ['tradeDate', 'cash', 'security_position',
                    'portfolio_value', 'benchmark_return', 'blotter',
                    'current_date', 'position', 'benchmark']:
            raise Errors.INVALID_OBSERVE_VARIABLES
        self.record[name] = deepcopy(value)
