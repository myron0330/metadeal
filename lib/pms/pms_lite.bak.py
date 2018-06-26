# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: PMS lite manager file
#   Author: Myron
# **********************************************************************************#
import logging
import numpy as np
from copy import copy
from datetime import datetime
from .. core.enums import AccountType
from .. trade.cost import Commission
from .. trade.order import (
    choose_order,
    OrderState,
    OrderStateMessage,
)
from .. trade.position import choose_position
from .. trade.portfolio import choose_portfolio
from .. trade.trade import choose_trade
from .. trade.transaction import (
    CashDividend,
    FundSplit,
    CashTransfer,
    Allotment,
    FundDividendCash, FundDividendShare, StockDividend, ForceOffset, DueOffset)
from .. utils.dict_utils import (
    DefaultDict,
    CompositeDict
)
from .. utils.error_utils import Errors
from .. const import EXCHANGE_FUND_PATTERN
from .. core.enums import FundClass


class PMSLite(object):

    """
    组合管理模块

    * 管理账户的持仓信息
    * 管理账户的订单委托信息
    * 管理账户的成交回报信息
    """
    def __init__(self, clock=None, accounts=None, data_portal=None, cash_info=None,
                 position_info=None, initial_value_info=None, initial_orders_info=None,
                 order_info=None, trade_info=None, portfolio_value_info=None,
                 benchmark_info=None, total_commission_info=None, market_roller=None,
                 settlement_info=None, observe_info=None):
        """
        组合管理配置

        Args:
            clock(clock): 时钟
            accounts(dict): 账户管理
            data_portal(data_portal): 数据模块
            cash_info(dict): 账户现金信息 |-> dict(account: dict(date: float))
            position_info(dict): 账户持仓信息 |-> dict(account: dict(date: dict))
            initial_value_info(dict): 初始权益信息 |-> dict(account: dict)
            initial_orders_info(dict): 初始订单信息 |-> dict(account: dict(date: dict))
            order_info(dict): 订单委托 |-> dict(account: dict(date: list))
            trade_info(dict): 成交记录 |-> dict(account: dict(date: list))
            portfolio_value_info(dict): 用户权益信息 |-> dict(account: dict(date: float))
            benchmark_info(dict): 用户对比权益曲线 |-> dict(account: string)
            total_commission_info(dict): 手续费记录　｜-> dict(account: dict(date: float))
            settlement_info(dict): 到期分红配股强平记录 | -> dict(account: dict(data: list))
            observe_info(dict): 策略观测的变量值
        """
        self.clock = clock
        self.accounts = accounts
        self.data_portal = data_portal
        self.cash_info = cash_info or DefaultDict(dict)
        self.position_info = position_info or DefaultDict(DefaultDict(dict))
        self.initial_value_info = initial_value_info or DefaultDict(dict)
        self.initial_orders_info = initial_orders_info or DefaultDict(dict)
        self.order_info = order_info or DefaultDict(DefaultDict(dict))
        self.pending_order_info = order_info or DefaultDict(DefaultDict(dict))
        self.trade_info = trade_info or DefaultDict(DefaultDict(list))
        self.portfolio_value_info = portfolio_value_info or DefaultDict(dict)
        self.benchmark_info = benchmark_info or dict()
        self.total_commission_info = total_commission_info or DefaultDict(DefaultDict(0))
        self.market_roller = market_roller
        self.settlement_info = settlement_info or DefaultDict(DefaultDict(list))
        self.observe_info = observe_info or DefaultDict(dict)

    @classmethod
    def from_config(cls, clock, sim_params, data_portal, market_roller, accounts=None):
        """
        从配置中生而成 PMSManager
        """
        cash_info = DefaultDict(dict)
        position_info = DefaultDict(DefaultDict(dict))
        initial_value_info = DefaultDict(dict)
        initial_orders_info = DefaultDict(dict)
        portfolio_value_info = DefaultDict(dict)
        total_commission_info = DefaultDict(DefaultDict(0))
        benchmark_info = dict()
        settlement_info = DefaultDict(DefaultDict(dict))
        observe_info = DefaultDict(dict)
        for account, config in accounts.iteritems():
            account_type = config.account_type
            capital_base = config.capital_base
            position_base = config.position_base
            cost_base = copy(config.cost_base)
            cash_info[account][sim_params.trading_days[0]] = float(capital_base)
            total_commission_info[account][sim_params.trading_days[0]] = 0
            position_obj = choose_position(account_type)
            pre_price_dict = dict()
            market_data = data_portal.market_service.get_market_data(account_type)
            if market_data:
                daily_bars = market_data.daily_bars
                if daily_bars:
                    if account_type == AccountType.futures:
                        data = daily_bars['settlementPrice']
                    elif account_type == AccountType.otc_fund:
                        data = daily_bars['nav']
                    else:
                        data = daily_bars['closePrice']
                    # get previous trading day of trading_days[0]
                    first_previous = data_portal.calendar_service.previous_trading_day_map[sim_params.trading_days[0]]
                    pre_price_dict = dict(data.loc[first_previous.strftime('%Y-%m-%d')])

            if set(cost_base) < set(position_base):
                for symbol in set(position_base) - set(cost_base):
                    cost_base[symbol] = pre_price_dict.get(symbol, 0)
            if account_type in [AccountType.security, AccountType.otc_fund]:
                position_info[account][sim_params.trading_days[0]] = \
                    {symbol: position_obj(symbol=symbol, amount=amount, available_amount=amount, cost=cost_base[symbol])
                     for symbol, amount in position_base.iteritems()}
                portfolio_value_info[account][sim_params.trading_days[0]] = \
                    capital_base + sum({position * cost_base[symbol] for symbol, position in position_base.iteritems()})
            elif account_type == AccountType.futures:
                initial_positions = dict()
                portfolio_value = float(capital_base)
                for symbol, amount in position_base.iteritems():
                    position = position_obj()
                    price = cost_base.get(symbol, 0)
                    if amount > 0:
                        position.long_amount = abs(amount)
                        position.long_cost = price
                    if amount < 0:
                        position.short_amount = abs(amount)
                        position.short_cost = price
                    position.price = pre_price_dict.get(symbol, 0)
                    initial_positions[symbol] = position
                    margin_rate, commission, multiplier, min_change_price, slippage = \
                        data_portal.asset_service.get_asset_info(symbol).get_trade_params()
                    portfolio_value += price * amount * multiplier * margin_rate
                position_info[account][sim_params.trading_days[0]] = initial_positions
                portfolio_value_info[account][sim_params.trading_days[0]] = portfolio_value
            elif account_type == AccountType.index:
                initial_positions = dict()
                portfolio_value = capital_base
                for symbol, amount in position_base.iteritems():
                    price = cost_base.get(symbol, 0)
                    position = position_obj(symbol, last_price=price)
                    cost = config.cost_base.get(symbol) or price
                    if amount > 0:
                        position.long_amount = abs(amount)
                        position.long_cost = cost
                        portfolio_value += price * amount
                    if amount < 0:
                        position.short_amount = abs(amount)
                        position.short_cost = cost
                        position.short_margin = cost * abs(amount)
                        portfolio_value += 2 * position.short_margin - price * abs(amount)
                    initial_positions[symbol] = position
                position_info[account][sim_params.trading_days[0]] = dict()
                portfolio_value_info[account][sim_params.trading_days[0]] = portfolio_value
            else:
                raise Errors.INVALID_ACCOUNT_TYPE
            initial_value_info[account] = {'cash': copy(cash_info[account][sim_params.trading_days[0]]),
                                           'positions': copy(position_info[account][sim_params.trading_days[0]]),
                                           'portfolio_value':
                                               copy(portfolio_value_info[account][sim_params.trading_days[0]])}
            initial_orders_info[account] = dict()
            benchmark_info[account] = sim_params.major_benchmark
        return cls(clock=clock, accounts=accounts, data_portal=data_portal, cash_info=cash_info,
                   position_info=position_info, initial_value_info=initial_value_info,
                   initial_orders_info=initial_orders_info,
                   portfolio_value_info=portfolio_value_info, benchmark_info=benchmark_info,
                   total_commission_info=total_commission_info, market_roller=market_roller,
                   settlement_info=settlement_info, observe_info=observe_info)

    def pre_trading_day(self, with_dividend=False, with_allot=False):
        """
        盘前处理

        Args:
            with_dividend(boolean): whether to do dividend.
            with_allot(boolean): 是否配股处理
        """
        for account, config in self.accounts.iteritems():
            self._synchronize_portfolio(account, config)
            account_type = config.account_type
            if account_type == AccountType.security:
                if with_dividend:
                    self._execute_dividend(account, config)
                if with_allot:
                    self._execute_allot(account, config)
            elif account_type == AccountType.futures:
                self._check_margin(account, config)

    def synchronize_broker(self, feedback_info):
        """
        当日同步撮合回报

        Args:
            feedback_info(dict): 信息回报
        """
        current_date = self.clock.current_date
        for account, account_cfg in self.accounts.iteritems():
            self.cash_info[account].update(feedback_info['cash'][account])
            self.position_info[account].update(feedback_info['positions'][account])
            self.order_info[account][current_date].update(feedback_info['orders'][account][current_date])
            self.pending_order_info[account][current_date].update(feedback_info['orders'][account][current_date])
            self.portfolio_value_info[account].update(feedback_info['portfolio_value'][account])
            # if account_cfg.account_type in [AccountType.index, AccountType.otc_fund]:
            #     self.settlement_info[account][current_date] += feedback_info['trades'][account][current_date]
            self.trade_info[account][current_date] += feedback_info['trades'][account][current_date]
            self.total_commission_info[account].update(feedback_info['total_commission'][account])
            self.settlement_info[account][current_date] += feedback_info['transactions'][account][current_date]

    def synchronize_pms(self, portfolio_info):
        """
        Synchronize pms info.

        Args:
            portfolio_info(dict): portfolio info
        """
        for account, info in portfolio_info.iteritems():
            current_date = datetime.strptime(info['date'], '%Y%m%d')
            config = self.accounts.get(account)
            if not config:
                continue
            account_type = config.account_type
            position_obj = choose_position(account_type)
            order_obj = choose_order(account_type)
            if info['cash']:
                self.cash_info[account][current_date] = info['cash']
            if info['portfolio_value']:
                self.portfolio_value_info[account][current_date] = info['portfolio_value']
            if info['orders']:
                orders = {
                    order_id: order_obj.from_pms(order) for order_id, order in info['orders'].iteritems()
                }
                self.order_info[account][current_date].update(orders)
            if info['positions']:
                positions = {
                    symbol: position_obj.from_pms(position) for symbol, position in info['positions'].iteritems()
                }
                self.position_info[account][current_date].update(positions)

    def evaluate_portfolio(self, settle=False):
        """
        Evaluate all accounts positions.
        settle(bool): 是否为结算操作
        """
        for account, config in self.accounts.iteritems():
            self._evaluate(account, config, settle=settle)

    def post_trading_day(self, feedback_info=None, dividend=False, split=False):
        """
        盘后处理

        Args:
            feedback_info(dict): 反馈信息
            dividend(bool): 场外基金分红信号
            split(bool): 场外基金拆分信号
        """
        if feedback_info:
            self.synchronize_broker(feedback_info=feedback_info)
        for account, config in self.accounts.iteritems():
            self.settlement(account, config)
            account_type = config.account_type
            if account_type == AccountType.security:
                self._record_dividend(account, config)
                self._record_allot(account, config)
                if split:
                    self._execute_split(account, config)
                    # 除权除息日当日一般停牌，当日无量价，不更新组合的权益市值，沿用停盘前最后的市值
            elif account_type == AccountType.otc_fund:
                self._record_dividend(account, config)
                # 场外基金权益登记日+1结算时执行分红操作
                if dividend:
                    self._execute_dividend(account, config)
                    # 先进行权益登记->执行分红->再做结算
                    self._evaluate(account, config, settle=True)
                if split:
                    self._execute_split(account, config)
                    self._evaluate(account, config, settle=True)

    def settlement(self, account, config):
        """
        Settlement: settle the portfolio value of all accounts
        """
        date = self.clock.current_date
        account_type = config.account_type
        # 结算时，先处理强平股票
        if account_type in [AccountType.security, AccountType.futures, AccountType.index]:
            self._close_expiration(account, config)
        portfolio_info = self.get_portfolio_info(account=account, info_date=date)[account]
        portfolio = choose_portfolio(account_type).from_portfolio_info(account, portfolio_info)
        custom_properties = {
            'commission': config.commission,
            'slippage': config.slippage,
            'margin_rate': config.margin_rate
        }
        feed_info = portfolio.settle(self.clock, self.data_portal, self.market_roller,
                                     custom_properties=custom_properties, portfolio_info=portfolio_info)
        # 场外基金在settlement时更新transaction
        if account_type == AccountType.otc_fund:
            self.settlement_info[account][date] += feed_info
        self.cash_info[account][date] = portfolio.cash
        self.portfolio_value_info[account][date] = portfolio.portfolio_value
        portfolio.remove_empty_positions()
        self.position_info[account][date] = portfolio.positions

    def has_pending_orders(self):
        """
        Check daily pending orders
        """
        for account, order_info in self.order_info.iteritems():
            today_orders = order_info[self.clock.current_date]
            for order_id, order in today_orders.iteritems():
                if order.state in OrderState.ACTIVE:
                    return True
        return False

    def get_portfolio_info(self, account=None, info_date=None):
        """
        获取当前时刻用户权益

        Args:
            account(string): account name
            info_date(datetime.datetime): Optional, 交易日期
        """
        zipped_data = CompositeDict()
        accounts = [account] if account else self.accounts.keys()
        for account in accounts:
            cash = self.cash_info[account].get(info_date)
            orders = self.order_info[account].get(info_date, dict())
            pending_orders = self.pending_order_info[account].get(info_date, dict())
            positions = self.position_info[account].get(info_date, dict())
            portfolio_value = self.portfolio_value_info[account].get(info_date)
            trades = self.trade_info[account].get(info_date, list())
            total_commission = self.total_commission_info[account].get(info_date)
            previous_date = self.data_portal.calendar_service.get_direct_trading_day(info_date, 1, forward=False)
            previous_positions = \
                self.position_info[account].get(previous_date, self.initial_value_info[account]['positions'])
            previous_portfolio_value = \
                self.portfolio_value_info[account].get(previous_date,
                                                       self.initial_value_info[account]['portfolio_value'])
            zipped_data[account]['previous_positions'] = previous_positions
            zipped_data[account]['previous_portfolio_value'] = previous_portfolio_value
            zipped_data[account]['initial_value'] = self.initial_value_info[account]
            zipped_data[account]['orders'] = orders
            zipped_data[account]['pending_orders'] = pending_orders
            zipped_data[account]['cash'] = cash
            zipped_data[account]['positions'] = positions
            zipped_data[account]['portfolio_value'] = portfolio_value
            zipped_data[account]['trades'] = trades
            zipped_data[account]['total_commission'] = total_commission
        return zipped_data

    def to_dict(self):
        """
        Returns:
            dict: PMS 信息汇总
        """
        return {
            'accounts': self.accounts,
            'cash': self.cash_info,
            'orders': self.order_info,
            'positions': self.position_info,
            'initial_value': self.initial_value_info,
            'portfolio_value': self.portfolio_value_info,
            'benchmark': self.benchmark_info,
            'total_commission': self.total_commission_info
        }

    def _synchronize_portfolio(self, account=None, config=None):
        """
        Synchronize portfolio info.

        Args:
            account(string): account name
            config(obj): AccountConfig
        """
        account_type = config.account_type
        self.cash_info[account][self.clock.current_date] = \
            self.cash_info[account].get(self.clock.previous_date,
                                        self.initial_value_info[account]['cash'])
        self.settlement_info[account][self.clock.current_date] = []
        previous_position_info = self.position_info[account].get(self.clock.previous_date,
                                                                 self.initial_value_info[account]['positions'])
        for symbol, position in previous_position_info.iteritems():
            current_position = copy(position)
            if account_type == AccountType.security:
                current_position.available_amount = current_position.amount
            if account_type == AccountType.futures:
                current_position.today_profit = 0
            self.position_info[account][self.clock.current_date][symbol] = current_position
        self.portfolio_value_info[account][self.clock.current_date] = \
            self.portfolio_value_info[account].get(self.clock.previous_date,
                                                   self.initial_value_info[account]['portfolio_value'])
        if account_type == AccountType.otc_fund:
            # 将场外基金未处理的订单放到下一交易日
            previous_order_info = self.pending_order_info[account].get(self.clock.previous_date,
                                                                       self.initial_orders_info[account])
            for order_id, order in previous_order_info.iteritems():
                current_order = copy(order)
                if current_order.purchase_confirming_date is not None and \
                        current_order.purchase_confirming_date >= self.clock.current_date:
                    self.pending_order_info[account][self.clock.current_date][order_id] = current_order
                elif current_order.redeem_confirming_date is not None and \
                        current_order.redeem_confirming_date >= self.clock.current_date:
                    self.pending_order_info[account][self.clock.current_date][order_id] = current_order
                elif current_order.switch_confirming_date is not None and \
                        current_order.switch_confirming_date >= self.clock.current_date:
                    self.pending_order_info[account][self.clock.current_date][order_id] = current_order

    def _evaluate(self, account, config, **kwargs):
        """

        Args:
            account(string): account name
            config(obj): AccountConfig
            **kwargs: key-value parameters
        """
        # todo. adapt to date and minute.
        date = self.clock.current_date
        account_type = config.account_type
        portfolio_info = self.get_portfolio_info(account=account, info_date=date)[account]
        portfolio = choose_portfolio(account_type).from_portfolio_info(account, portfolio_info)
        custom_properties = {
            'commission': config.commission,
            'slippage': config.slippage,
            'margin_rate': config.margin_rate
        }
        portfolio.evaluate_profit(self.clock, self.data_portal, self.market_roller,
                                  custom_properties=custom_properties, **kwargs)
        self.cash_info[account][date] = portfolio.cash
        self.portfolio_value_info[account][date] = portfolio.portfolio_value
        self.position_info[account][date].update(portfolio.positions)

    def _execute_dividend(self, account, config):
        """
        execute dividend by a specific account and config

        Args:
            account(string): account name
            config(obj): AccountConfig
        """
        date = self.clock.current_date
        date_string = date.strftime('%Y-%m-%d')
        market_data = self.data_portal.market_service.get_market_data(config.account_type)
        if not market_data or not market_data.dividends:
            return
        dividend_data = market_data.dividends

        if config.account_type == AccountType.security:
            cash_div, share_div = dividend_data.get('cash_div'), dividend_data.get('share_div')
            positions = self.position_info[account][date]
            if not self.cash_info[account][date] or not positions:
                return
            if cash_div and cash_div.get(date_string):
                cash_div_info = cash_div[date_string]
                for symbol, position in positions.iteritems():
                    if symbol in cash_div_info and position.dividends:
                        dividend_ratio = cash_div_info.get(symbol, 0)
                        cash_amount = dividend_ratio * position.dividends.dividend_amount
                        self.cash_info[account][date] += cash_amount
                        position.cost -= cash_div_info.get(symbol, 0)
                        tx_obj = FundDividendCash if EXCHANGE_FUND_PATTERN.match(symbol) else CashDividend
                        transaction = tx_obj(symbol=symbol, transact_amount=position.dividends.dividend_amount,
                                             dividend_cash=cash_amount, dividend_ratio=dividend_ratio,
                                             transact_time=date_string, account=account)
                        self.settlement_info[account][date].append(transaction)
            if share_div and share_div.get(date_string):
                share_div_info = share_div[date_string]
                for symbol, position in positions.iteritems():
                    if symbol in share_div_info and position.dividends:
                        share_ratio = share_div_info.get(symbol, 1) - 1
                        share_amount = round(position.dividends.dividend_amount * share_ratio)
                        if share_amount == 0:
                            continue
                        position.amount += share_amount
                        position.cost /= share_div_info.get(symbol, 1)
                        transaction = StockDividend(symbol=symbol, transact_amount=share_amount,
                                                    transfer_ratio=share_ratio, transact_time=date_string,
                                                    account=account)
                        self.settlement_info[account][date].append(transaction)
        elif config.account_type == AccountType.otc_fund:
            otc_fund_dividend_data = dividend_data.get('div_rate')
            positions = self.position_info[account][date]
            if not self.cash_info[account][date] or not positions:
                return
            if otc_fund_dividend_data and otc_fund_dividend_data.get(date_string):
                div_info = otc_fund_dividend_data[date_string]
                for symbol, position in positions.iteritems():
                    if position.dividends is None:
                        continue

                    cash_ratio = div_info.get(symbol, 0)
                    cash_bonus = cash_ratio * position.dividends.dividend_amount
                    if config.dividend_method == "cash_bonus":
                        self.cash_info[account][date] += cash_bonus
                        # 场外基金分红处理在净值除权除息后，计算portfolio_value时，使用除权后的净值计算权益
                        # 分红处理时应对把分红金额补回到portfolio_value
                        self.portfolio_value_info[account][date] += cash_bonus
                        position.cost -= cash_ratio
                        transaction = FundDividendCash(symbol=symbol,
                                                       transact_amount=position.dividends.dividend_amount,
                                                       dividend_cash=cash_bonus, dividend_ratio=cash_ratio,
                                                       transact_time=date_string, account=account)
                        self.settlement_info[account][date].append(transaction)
                    elif config.dividend_method == "reinvestment":
                        self.portfolio_value_info[account][date] += cash_bonus
                        portfolio_info = self.get_portfolio_info(account=account, info_date=date)[account]
                        portfolio = choose_portfolio(config.account_type).from_portfolio_info(account, portfolio_info)
                        pre_date = self.clock.previous_date
                        nav = self.market_roller.tas_daily_expanded_cache[pre_date]["nav"][symbol]
                        self.position_info[account][date][symbol].value += cash_bonus
                        order_amount = round(cash_bonus / nav, 2)
                        portfolio.update_position(symbol, order_amount, nav, 'purchase')
                        # position.amount += order_amount
                        # position.cost /= div_info.get(symbol, 1)
                        transaction = FundDividendShare(symbol=symbol,
                                                        transact_amount=position.dividends.dividend_amount,
                                                        dividend_share=order_amount, dividend_ratio=cash_ratio,
                                                        transact_time=date_string, account=account)
                        self.settlement_info[account][date].append(transaction)

    def _record_dividend(self, account, config):
        """
        Record dividend by account and config

        Args:
            account(string): account name
            config(obj): AccountConfig
        """
        market_data = self.data_portal.market_service.get_market_data(config.account_type)
        if not market_data or not market_data.dividends:
            return
        dividend_data = market_data.dividends
        date_string = self.clock.current_date.strftime('%Y-%m-%d')
        div_record = dividend_data.get('div_record')
        div_record_info = div_record.get(date_string, None) if div_record else None
        if not div_record_info:
            return
        if config.account_type == AccountType.security:
            positions = self.position_info[account][self.clock.current_date]
            for symbol, position in positions.iteritems():
                if symbol in div_record_info:
                    position.dividends = Dividend(dividend_amount=position.amount,
                                                  expiring_date=div_record_info[symbol])
                if not position.dividends:
                    continue
                expiring_date = position.dividends.expiring_date
                if not expiring_date \
                        or expiring_date.strftime('%Y-%m-%d') <= self.clock.current_date.strftime('%Y-%m-%d'):
                    position.dividends = None
        if config.account_type == AccountType.otc_fund:
            positions = self.position_info[account][self.clock.current_date]
            for symbol, position in positions.iteritems():
                if symbol in div_record_info:
                    position.dividends = Dividend(dividend_amount=position.amount,
                                                  expiring_date=div_record_info[symbol])
                if not position.dividends:
                    continue
                expiring_date = position.dividends.expiring_date
                if not expiring_date \
                        or expiring_date.strftime('%Y-%m-%d') <= self.clock.current_date.strftime('%Y-%m-%d'):
                    position.dividends = None

    def _record_allot(self, account, config):
        """
        Record dividend by account and config

        Args:
            account(string): account string
            config(obj): AccountConfig
        """
        market_data = self.data_portal.market_service.get_market_data(config.account_type)
        if not market_data or not market_data.allots:
            return
        allot_data = market_data.allots
        date_string = self.clock.current_date.strftime('%Y-%m-%d')
        allot_record = allot_data.get('allot_record')
        allot_record_info = allot_record.get(date_string, None) if allot_record else None
        if not allot_record_info:
            return
        if config.account_type == AccountType.security:
            positions = self.position_info[account][self.clock.current_date]
            for symbol, position in positions.iteritems():
                if symbol in allot_record_info:
                    position.allots = Allot(allot_amount=position.amount,
                                            expiring_date=allot_record_info[symbol])
                    if not position.allots:
                        continue
                    expiring_date = position.allots.expiring_date
                    if not expiring_date \
                            or expiring_date.strftime('%Y-%m-%d') <= self.clock.current_date.strftime('%Y-%m-%d'):
                        position.allots = None

    def _execute_allot(self, account, config):
        """
        execute dividend by a specific account and config

        Args:
            account(string): account string
            config(obj): AccountConfig
        """
        date = self.clock.current_date
        date_string = date.strftime('%Y-%m-%d')
        market_data = self.data_portal.market_service.get_market_data(config.account_type)
        if not market_data or not market_data.allots:
            return

        allot_data = market_data.allots

        if config.account_type == AccountType.security:
            share_allot, price_allot = allot_data.get('share_allot'), allot_data.get('price_allot')
            positions = self.position_info[account][date]
            if not self.cash_info[account][date] or not positions:
                return
            if share_allot and share_allot.get(date_string):
                share_allot_info = share_allot[date_string]
                for symbol, position in positions.iteritems():
                    if symbol in share_allot_info and position.allots:
                        allot_amount = round(position.allots.allot_amount * (share_allot_info.get(symbol, 0)))
                        allot_price = price_allot[date_string].get(symbol, 0)
                        if self.cash_info[account][date] >= allot_amount*allot_price:
                            allot_ratio = share_allot_info.get(symbol, 0)
                            amount = round(position.allots.allot_amount * allot_ratio)
                            position.amount += amount
                            position.cost = (position.value + allot_amount*allot_price)/(position.amount+allot_amount)
                            position.value += allot_amount*allot_price
                            self.cash_info[account][date] -= allot_amount*allot_price
                            transaction = Allotment(symbol=symbol, transact_price=allot_price, transact_amount=amount,
                                                    allot_ratio=allot_ratio, transact_time=date_string, account=account)
                            self.settlement_info[account][date].append(transaction)
                        else:
                            logging.info(u'股票{} 配股订单未成功撮合!'.format(symbol))

    def _execute_split(self, account, config):
        """
        execute split by a specific account and config

        Args:
            account(string): account string
            config(obj): AccountConfig
        """
        market_data = self.data_portal.market_service.get_market_data(config.account_type)
        if not market_data or not market_data.splits:
            return
        split_data = market_data.splits
        date = self.clock.current_date
        date_string = self.clock.current_date.strftime('%Y-%m-%d')
        share_split = split_data.get('share_split')
        split_info = share_split.get(date_string, None) if share_split else None
        if not split_info:
            return
        if config.account_type == AccountType.otc_fund:
            positions = self.position_info[account][date]
            if not positions:
                return
            for symbol, position in positions.iteritems():
                if symbol in split_info:
                    redeem_dividend_amount = sum(map(lambda order: order.filled_amount,
                                                     filter(lambda order: order.order_time == self.clock.current_date
                                                            and order.order_type == "redeem",
                                                            self.pending_order_info[account][date_string].values())))
                    to_split_amount = redeem_dividend_amount + position.amount
                    position.amount = round(to_split_amount*split_info.get(symbol, 1), 2)
                    position.available_amount = round(to_split_amount*split_info.get(symbol, 1), 2)
                    position.cost /= split_info.get(symbol, 1)
                    transaction = FundSplit(symbol=symbol, transact_amount=to_split_amount,
                                            transact_time=date_string, account=account)
                    # transaction = FundSplit(symbol=symbol, direction=1, transact_amount=to_split_amount,
                    #                         transact_price=0, transact_time=date_string)
                    self.settlement_info[account][date].append(transaction)
        elif config.account_type == AccountType.security:
            positions = self.position_info[account][date]
            if not positions:
                return
            for symbol, position in positions.iteritems():
                if symbol in split_info:
                    fund_class = self.data_portal.asset_service.get_asset(symbol).is_structured
                    # TODO 未处理分级A的拆分
                    if fund_class == FundClass.NA or fund_class == FundClass.STRUCTURED_B:
                        to_split_amount = position.amount
                        position.amount = round(to_split_amount * split_info.get(symbol, 1), 2)
                        position.available_amount = round(to_split_amount * split_info.get(symbol, 1), 2)
                        position.cost /= split_info.get(symbol, 1)
                        transaction = FundSplit(symbol=symbol, transact_amount=to_split_amount,
                                                transact_time=date_string, account=account)
                        # transaction = FundSplit(symbol=symbol, direction=1, transact_amount=to_split_amount,
                        #                         transact_price=0, transact_time=date_string)
                        self.settlement_info[account][date].append(transaction)

    def _check_margin(self, account, config):
        """
        Check margin on current account.

        Args:
            account(string): account name
        """
        date = self.clock.current_date
        account_type = config.account_type
        if account_type == AccountType.futures:
            available_cash = self.cash_info[account].get(date)
            if available_cash < 0:
                target_positions = copy(self.position_info[account].get(date, dict()))
                self._close_positions(account, config, target_positions=target_positions, price_type='openPrice',
                                      futures_force_offset=True)
        elif account_type == AccountType.index:
            raise NotImplementedError

    def _close_expiration(self, account, config):
        """
        Close expiration security.

        Args:
            account(string): account name
            config(obj): AccountConfig
        """
        date = self.clock.current_date
        positions = self.position_info[account].get(date, dict())

        def _expired_positions(symbol):
            """
            Check out the expired positions.

            Args:
                symbol(string): security symbol
            """
            return not self.data_portal.asset_service.get_asset(symbol, date).is_active_within(
                start=date, exclude_last_date=True)

        target_positions = \
            {symbol: position for symbol, position in positions.iteritems() if _expired_positions(symbol)}

        account_type = config.account_type
        if account_type == AccountType.security:
            price_type = 'closePrice'
        elif account_type == AccountType.futures:
            price_type = 'settlementPrice'
        elif account_type == AccountType.otc_fund:
            price_type = 'nav'
        elif account_type == AccountType.index:
            price_type = 'closePrice'
        else:
            raise Errors.INVALID_ACCOUNT_TYPE
        if target_positions:
            self._close_positions(account, config, target_positions=target_positions, price_type=price_type)

    def _close_positions(self, account, config, target_positions=None, price_type='openPrice',
                         futures_force_offset=False):
        """
        Close positions according to price_type.

        Args:
            account(string): account name
            config(obj): AccountConfig
            target_positions(dict): target positions that need to be closed.
            price_type(string): price type
            futures_force_offset(Boolean): 是否属于期货强平操作
        """
        date = self.clock.current_date
        previous_date = self.clock.previous_date
        account_type = config.account_type
        order_obj = choose_order(account_type)
        trade_obj = choose_trade(account_type)
        order_time = self.clock.now.strftime('%Y-%m-%d %H:%M')
        if account_type == AccountType.security:
            for symbol, position in target_positions.iteritems():
                amount = position.amount
                price = self.market_roller.tas_daily_expanded_cache[previous_date][price_type][symbol]
                if amount:
                    direction = -1
                    commission_obj = config.commission or Commission()
                    commission = abs(amount * commission_obj.calculate_stock_commission(price, direction))
                    slippage = 0
                    order = order_obj(
                        symbol=symbol,
                        amount=-amount,
                        order_time=order_time,
                        order_type='market',
                        state=OrderState.FILLED,
                        state_message=OrderStateMessage.FILLED,
                        filled_time=order_time,
                        filled_amount=amount,
                        turnover_value=amount*price,
                        transact_price=price,
                        commission=commission,
                        slippage=slippage
                    )
                    trade = trade_obj(order.order_id, order.symbol, order.direction, order.offset_flag, amount,
                                      order.transact_price, order.filled_time, commission, slippage)
                    self.order_info[account][date].update({order.order_id: order})
                    self.trade_info[account][date] += [trade]
                    self.cash_info[account][date] += price * amount - commission - slippage
                    self.position_info[account][date].pop(symbol)
                    transaction = DueOffset(symbol=symbol, position_direction=1, transact_price=price,
                                            transact_amount=amount, commission=commission,
                                            transact_time=order_time, account=account)
                    self.settlement_info[account][date].append(transaction)
        if account_type == AccountType.futures:
            for symbol, position in target_positions.iteritems():
                long_amount = position.long_amount
                short_amount = position.short_amount
                price = self.market_roller.tas_daily_expanded_cache[date][price_type][symbol]
                if np.isnan(price):
                    price = self.market_roller.tas_daily_expanded_cache[date]['preSettlementPrice'][symbol]
                    if np.isnan(price):
                        continue
                symbol_asset = self.data_portal.asset_service.get_asset(symbol, self.clock.current_date)
                margin_rate, commission_obj, multiplier, min_change_price, slippage_obj = \
                    symbol_asset.get_trade_params(custom_properties=dict())
                if long_amount:
                    market_value = price * multiplier * long_amount
                    commission = commission_obj.calculate_futures_commission(market_value, offset_flag='close')
                    slippage = 0
                    order = order_obj(
                        symbol=symbol,
                        order_amount=-long_amount,
                        order_type='market',
                        offset_flag='close',
                        direction=-1,
                        order_time=order_time,
                        filled_time=order_time,
                        filled_amount=long_amount,
                        turnover_value=long_amount*market_value,
                        state=OrderState.FILLED,
                        state_message=OrderStateMessage.FILLED,
                        transact_price=price,
                        commission=commission,
                        slippage=slippage
                    )
                    trade = trade_obj(order, order.transact_price, long_amount, margin_rate,
                                      commission, multiplier, slippage, order_time)
                    self.order_info[account][date].update({order.order_id: order})
                    self.trade_info[account][date] += [trade]
                    close_pnl = position.calc_close_pnl(trade, multiplier)
                    self.portfolio_value_info[account][date] += close_pnl - commission
                    self.cash_info[account][date] += market_value * margin_rate + close_pnl - commission
                    tx_obj = ForceOffset if futures_force_offset else DueOffset
                    transaction = tx_obj(symbol=symbol, position_direction=1, transact_price=price,
                                         transact_amount=long_amount, commission=commission,
                                         transact_time=order_time, account=account)
                    self.settlement_info[account][date].append(transaction)
                if short_amount:
                    market_value = price * multiplier * short_amount
                    commission = commission_obj.calculate_futures_commission(market_value, offset_flag='close')
                    slippage = 0
                    order = order_obj(
                        symbol=symbol,
                        order_amount=short_amount,
                        offset_flag='close',
                        direction=1,
                        order_time=order_time,
                        filled_time=order_time,
                        filled_amount=short_amount,
                        turnover_value=short_amount * market_value,
                        state=OrderState.FILLED,
                        state_message=OrderStateMessage.FILLED,
                        transact_price=price,
                        commission=commission,
                        slippage=slippage
                    )
                    trade = trade_obj(order, order.transact_price, short_amount, margin_rate,
                                      commission, multiplier, slippage, order_time)
                    self.order_info[account][date].update({order.order_id: order})
                    self.trade_info[account][date] += [trade]
                    close_pnl = position.calc_close_pnl(trade, multiplier)
                    self.portfolio_value_info[account][date] += close_pnl - commission
                    self.cash_info[account][date] += market_value * margin_rate + close_pnl - commission
                    tx_obj = ForceOffset if futures_force_offset else DueOffset
                    transaction = tx_obj(symbol=symbol, position_direction=-1, transact_price=price,
                                         transact_amount=short_amount, commission=commission,
                                         transact_time=order_time, account=account)
                    self.settlement_info[account][date].append(transaction)
                self.position_info[account][date].pop(symbol)
        elif account_type == AccountType.index:
            for symbol, position in target_positions.iteritems():
                long_amount = position.long_amount
                short_amount = position.short_amount
                price = self.market_roller.tas_daily_expanded_cache[date][price_type][symbol]
                if long_amount:
                    market_value = price * long_amount
                    commission_obj = config.commission or Commission()
                    commission = commission_obj.calculate_index_commission(market_value, offset_flag='close')
                    slippage = 0
                    order = order_obj(
                        symbol=symbol,
                        order_amount=-long_amount,
                        order_type='market',
                        offset_flag='close',
                        direction=-1,
                        order_time=order_time,
                        filled_time=order_time,
                        filled_amount=long_amount,
                        turnover_value=long_amount*market_value,
                        state=OrderState.FILLED,
                        state_message=OrderStateMessage.FILLED,
                        transact_price=price,
                        commission=commission,
                        slippage=slippage
                    )
                    trade = trade_obj(order, order.transact_price, long_amount, commission, order_time)
                    self.order_info[account][date].update({order.order_id: order})
                    self.trade_info[account][date] += [trade]
                    close_pnl = long_amount * (price - position.last_price)
                    self.portfolio_value_info[account][date] += close_pnl - commission
                    self.cash_info[account][date] += market_value - commission
                    transaction = DueOffset(symbol=symbol, position_direction=1, transact_price=price,
                                            transact_amount=long_amount, commission=commission,
                                            transact_time=order_time, account=account)
                    self.settlement_info[account][date].append(transaction)
                if short_amount:
                    market_value = price * short_amount
                    commission_obj = config.commission or Commission()
                    commission = commission_obj.calculate_futures_commission(market_value, offset_flag='close')
                    slippage = 0
                    order = order_obj(
                        symbol=symbol,
                        order_amount=short_amount,
                        offset_flag='close',
                        direction=1,
                        order_time=order_time,
                        filled_time=order_time,
                        filled_amount=short_amount,
                        turnover_value=short_amount * market_value,
                        state=OrderState.FILLED,
                        state_message=OrderStateMessage.FILLED,
                        transact_price=price,
                        commission=commission,
                        slippage=slippage
                    )
                    trade = trade_obj(order, order.transact_price, short_amount, commission, order_time)
                    self.order_info[account][date].update({order.order_id: order})
                    self.trade_info[account][date] += [trade]
                    close_pnl = - short_amount * (price - position.last_price)
                    self.portfolio_value_info[account][date] += close_pnl - commission
                    self.cash_info[account][date] += 2 * position.short_margin - market_value - commission - slippage
                    transaction = DueOffset(symbol=symbol, position_direction=-1, transact_price=price,
                                            transact_amount=short_amount, commission=commission,
                                            transact_time=order_time, account=account)
                    self.settlement_info[account][date].append(transaction)
                self.position_info[account][date].pop(symbol)

    def do_cash_inout(self, cash_orders):
        """
        执行各个账户间现金划转

        Args:
            cash_orders(deque): 账户现金划入划出记录

        """
        date = self.clock.current_date
        for account_name, cash_inout_deque in cash_orders.iteritems():
            while cash_inout_deque:
                account_from, account_to, cash_amount = cash_inout_deque.popleft()
                self.cash_info[account_name][date] += cash_amount
                self.portfolio_value_info[account_name][date] += cash_amount
                transaction = CashTransfer(transact_time=date.strftime('%Y-%m-%d'),
                                           account_from=account_from,
                                           account_to=account_to,
                                           transact_amount=cash_amount,
                                           account=account_name)
                self.settlement_info[account_name][date].append(transaction)
                # log

    # def _del_expired_position_dividend(self, account, config):
    #     """
    #     Record dividend by account and config
    #
    #     Args:
    #         account(string): account name
    #         config(obj): AccountConfig
    #     """
    #     if config.account_type == AccountType.security:
    #         positions = self.position_info[account][self.clock.current_date]
    #         for _, position in positions.iteritems():
    #             if not position.dividends:
    #                 continue
    #             else:
    #                 expiring_date = position.dividends.expiring_date
    #                 if expiring_date.strftime('%Y-%m-%d') <= self.clock.current_date.strftime('%Y-%m-%d'):
    #                     position.dividends = None
