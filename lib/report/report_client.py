# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: ReportClient file
# **********************************************************************************#
from __future__ import division
import pandas as pd
from . risk_metrics import *
from . report import choose_report
from .. universe.universe import UniverseService
from .. utils.error_utils import Errors
from .. utils.pandas_utils import smart_concat


def _generate_aggregated_bt(bt_by_account):
    """
    Generate aggregated bt

    Args:
        bt_by_account(dict): respective bt by account

    Returns:
        DataFrame: aggregated bt
    """
    if len(bt_by_account) == 0:
        raise ValueError('Backtest report is empty, please check the date range and history window!')
    elif len(bt_by_account) == 1:
        temp_bt = bt_by_account.values()[0]
        if temp_bt.columns.tolist().count('cash'):
            temp_bt.index = range(temp_bt.shape[0])
        aggregate_bt = temp_bt
    else:
        aggregate_bt = smart_concat(bt_by_account.itervalues(), axis=1)
        assert isinstance(aggregate_bt, pd.DataFrame), 'Invalid type of aggregate_bt.'
        aggregate_portfolio = aggregate_bt.portfolio_value.sum(axis=1)
        aggregate_bt = aggregate_bt.drop('portfolio_value', axis=1)
        aggregate_bt['portfolio_value'] = aggregate_portfolio
        aggregate_bt.index = range(aggregate_bt.shape[0])
        columns = aggregate_bt.columns if 'tradeDate' not in aggregate_bt.columns \
            else ['tradeDate'] + list(set(aggregate_bt.columns) - set(['tradeDate']))
        aggregate_bt = aggregate_bt[columns]
    aggregate_bt = aggregate_bt.loc[:, ~aggregate_bt.columns.duplicated()]
    return aggregate_bt


class BTReport(object):
    """
    BTReport.
    """
    def __init__(self, bt, perf, bt_by_account):
        """
        Args:
            bt(DataFrame): bt object.
            perf(dict): performance dict.
            bt_by_account(dict): bt by account dict.
        """
        self.bt = bt
        self.perf = perf
        self.bt_by_account = bt_by_account


class ReportClient(object):
    """
    Report client.
    """
    def __init__(self, sim_params, data_portal, pms_lite, market_roller):
        self.sim_params = sim_params
        self.data_portal = data_portal
        self.pms_lite = pms_lite
        self.market_roller = market_roller

    def output(self):
        """
        Generate bt report.
        """
        bt_by_account = self._generate_bt_by_account()
        bt = _generate_aggregated_bt(bt_by_account)
        trading_days = self.data_portal.calendar_service.trading_days
        initial_value = sum(map(lambda x: x['portfolio_value'], self.pms_lite.initial_value_info.values()))
        portfolio_value = bt.portfolio_value.tolist()
        benchmark_return = bt.benchmark_return
        security_position = list()
        with_turnover_rate = False
        if 'security_position' in bt.columns:
            security_position = bt['security_position'].tolist()
            with_turnover_rate = True
        perf = self._generate_perf(trading_days, initial_value,
                                   portfolio_value, benchmark_return,
                                   security_position=security_position,
                                   with_turnover_rate=with_turnover_rate)
        return BTReport(bt, perf, bt_by_account)

    def perf(self):
        """
        Generate performance.
        """
        initial_value = sum(map(lambda x: x['portfolio_value'],
                                self.pms_lite.initial_value_info.values()))
        portfolio_frame = pd.DataFrame(self.pms_lite.portfolio_value_info)
        trading_days = list(portfolio_frame.index)
        portfolio_value = portfolio_frame.sum(axis=1).tolist()
        benchmark = self.pms_lite.benchmark_info.values()[0]
        benchmark_return = self._get_benchmark_return(benchmark=benchmark,
                                                      trading_days=trading_days,
                                                      return_type='array')
        performance = self._generate_perf(trading_days, initial_value,
                                          portfolio_value, benchmark_return,
                                          with_turnover_rate=False)
        return performance

    def _generate_bt_by_account(self):
        """
        Generate bt by account

        Returns:
            dict: respective bt by account
        """
        bt_by_account = dict()
        for account, config in self.pms_lite.accounts.iteritems():
            cash = self.pms_lite.cash_info[account]
            trade_dates = {date: date for date in self.data_portal.calendar_service.trading_days}
            orders = {date: [_[key] for key in sorted(_)] for date, _ in self.pms_lite.order_info[account].iteritems()}
            orders.update({date: [] for date in trade_dates if date not in orders})
            positions = {date: {symbol: position.detail() for symbol, position in _.iteritems()}
                         for date, _ in self.pms_lite.position_info[account].iteritems()}
            portfolio_value = self.pms_lite.portfolio_value_info[account]
            benchmark = self.pms_lite.benchmark_info[account]
            benchmark_return = self._get_benchmark_return(benchmark=benchmark,
                                                          trading_days=sorted(trade_dates),
                                                          return_type='dict')
            trades = self.pms_lite.trade_info[account]
            report_obj = choose_report(config.account_type)
            report = report_obj(
                cash=cash,
                trade_dates=trade_dates,
                orders=orders,
                positions=positions,
                trades=trades,
                portfolio_value=portfolio_value,
                benchmark_return=benchmark_return
            ).output()
            bt_by_account[account] = report
        return bt_by_account

    def _generate_perf(self, trading_days, initial_value, portfolio_value,
                       benchmark_return, security_position=list(),
                       with_turnover_rate=True):
        """
        Generate performance parameters.

        Args:
            trading_days(array like): 交易日
            initial_value(numerical): initialize portfolio value
            portfolio_value(array like): 权益序列
            benchmark_return(array like): 基准收益率序列
            security_position(array like): 股票持仓序列
            with_turnover_rate(boolean): whether to calculate turnover rate
        Returns:
            dict: 回测表现报告
        """
        universe_service = self.data_portal.universe_service
        keys = ['annualized_return', 'volatility', 'returns', 'cumulative_values', 'cumulative_returns',
                'benchmark_annualized_return', 'benchmark_volatility', 'benchmark_returns',
                'benchmark_cumulative_values', 'benchmark_cumulative_returns', 'treasury_return', 'alpha', 'beta',
                'excess_return', 'sharpe', 'information_ratio',
                'information_coefficient', 'max_drawdown', 'turnover_rate']
        perf = {key: None for key in keys}
        st_returns = get_return([initial_value] + portfolio_value)
        bm_returns = benchmark_return.T.tolist()
        perf['returns'] = pd.Series(st_returns)
        perf['returns'].index = trading_days
        perf['benchmark_returns'] = pd.Series(bm_returns)
        perf['benchmark_returns'].index = trading_days
        # volatility
        st_vol = np.std(st_returns, ddof=1) * 250 ** 0.5
        if st_vol in [-np.inf, np.inf] or np.isnan(st_vol):
            perf['volatility'] = None
        else:
            perf['volatility'] = st_vol
        bm_vol = np.std(bm_returns, ddof=1) * 250 ** 0.5
        if st_vol in [-np.inf, np.inf] or np.isnan(st_vol):
            perf['benchmark_volatility'] = None
        else:
            perf['benchmark_volatility'] = bm_vol
        # cumulative values and cumulative returns
        c_st_values = get_cumulative_value(st_returns)
        c_bm_values = get_cumulative_value(bm_returns)
        perf['cumulative_values'] = pd.Series(c_st_values)
        perf['cumulative_values'].index = trading_days
        perf['cumulative_returns'] = pd.Series([v - 1 for v in c_st_values])
        perf['cumulative_returns'].index = trading_days
        perf['benchmark_cumulative_values'] = pd.Series(c_bm_values)
        perf['benchmark_cumulative_values'].index = trading_days
        perf['benchmark_cumulative_returns'] = pd.Series([v - 1 for v in c_bm_values])
        perf['benchmark_cumulative_returns'].index = trading_days
        perf['annualized_return'] = get_annualized_return(c_st_values)
        perf['benchmark_annualized_return'] = get_annualized_return(c_bm_values)
        rf = 0.035
        perf['treasury_return'] = rf
        perf['alpha'], perf['beta'] = get_CAPM(st_returns, bm_returns, rf)
        perf['excess_return'] = perf['annualized_return'] - perf['treasury_return']
        if perf['volatility'] is None:
            perf['sharpe'] = None
        else:
            sharpe = perf['excess_return'] / perf['volatility']
            if sharpe in [np.inf, -np.inf] or np.isnan(sharpe):
                perf['sharpe'] = None
            else:
                perf['sharpe'] = sharpe
        # information ratio and information coefficient
        perf['information_ratio'] = get_information_ratio(st_returns, bm_returns)
        if perf['information_ratio'] is None:
            perf['information_coefficient'] = None
        else:
            assert isinstance(universe_service, UniverseService)
            len_universe = len(universe_service.view(with_init_universe=True))
            perf['information_coefficient'] = perf['information_ratio'] / (len_universe * len(trading_days)) ** 0.5
        perf['max_drawdown'] = get_max_drawdown(c_st_values)
        perf['turnover_rate'] = 0
        if with_turnover_rate:
            perf['turnover_rate'] = self._get_turnover_rate(portfolio_value, security_position, trading_days)
        return perf

    def _get_turnover_rate(self, portfolio_value, security_position, trading_days):
        """
        Calculate turnover rate

        Args:
            portfolio_value: portfolio value
            security_position: security position
            trading_days: trading days
        """
        end_date = trading_days[-1]
        data = self.data_portal.market_service.slice(symbols='all', fields=['openPrice'],
                                                     end_date=end_date, time_range=len(security_position)
                                                     )['openPrice'].to_dict()
        buy_value, sell_value = 0, 0
        for pre_index, current_index in enumerate(range(len(security_position))[1:]):
            pre_position, current_position = \
                security_position[pre_index], security_position[current_index]
            for symbol in set(pre_position) | set(current_position):
                current_date = trading_days[current_index].strftime('%Y-%m-%d')
                if not pre_position.get(symbol):
                    buy_value += current_position[symbol]['amount'] * data[symbol][current_date]
                elif not current_position.get(symbol):
                    sell_value += \
                        pre_position[symbol]['amount'] * data[symbol][current_date]
                else:
                    amount_diff = current_position[symbol]['amount'] - pre_position[symbol]['amount']
                    value = \
                        abs(amount_diff) * data[symbol][current_date]
                    if amount_diff > 0:
                        buy_value += value
                    elif amount_diff < 0:
                        sell_value += value
        return get_turnover_rate(buy_value, sell_value, float(np.mean(portfolio_value)))

    def _get_benchmark_return(self, benchmark, trading_days, return_type='list'):
        """
        Get benchmark return

        Args:
            benchmark: benchmark
        """
        benchmark_data = self.data_portal.market_service.slice([benchmark], ['closePrice'], trading_days[-1],
                                                               time_range=len(trading_days)+1)
        if 'closePrice' in benchmark_data:
            benchmark_series = \
                np.array(benchmark_data['closePrice'])
        else:
            raise Errors.INVALID_BENCHMARK
        benchmark_return = benchmark_series[1:]/benchmark_series[:-1] - 1
        if return_type == 'dict':
            return dict(zip(trading_days, benchmark_return[:, 0].tolist()))
        return benchmark_return[:, 0]
