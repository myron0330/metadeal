# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Report file
# **********************************************************************************#
import pandas as pd
from copy import deepcopy, copy
from . risk_metrics import *
from .. utils.datetime_utils import get_trading_days
from .. universe.universe import UniverseService
from .. utils.pandas_utils import smart_concat
from .. data.asset_service import AssetType
from .. account import StockAccount, FuturesAccount, OTCFundAccount, IndexAccount


class StocksReport(object):
    """
    股票回测记录，包含如下属性

    * self.std_keys：标准输出变量名称
    * self.rec_keys：从account中记录的变量名称
    * self.sup_keys：通过observe增加的变量名称
    * self.tradeDate：记录日期列表
    * self.blotter：记录交易指令列表
    * self.cash：记录现金列表
    * self.security_position：记录证券头寸列表
    * self.portfolio_value：记录投资组合价值
    * self.benchmark_return：记录参照标准收益率列表
    * self.blotter: 记录指令簿列表
    * self.buy_value: 记录总买入价值列表
    * self.sell_value: 记录总卖出价值列表
    * self.initial_value: 记录账户初始值
    * self.len_universe: 记录回测总证券池数量
    """

    def __init__(self, data, sim_params, universe_service):
        """
        初始化，输入必须是在同一套参数下定义的

        Args:
            data (data): 数据行情
            sim_params (SimulationParameters): SimulationParameters实例
            universe_service (Universe): UniverseService实例

        Examples:
            >> report = StockReport(data, sim_params, universe)
        """

        self.std_keys = ['tradeDate', 'cash', 'security_position',
                         'portfolio_value', 'benchmark_return', 'blotter']
        self.rec_keys = ['current_date', 'position', 'benchmark', 'blotter']
        self.sup_keys = []
        self.tradeDate = []
        self.cash = []
        self.security_position = []
        self.portfolio_value = []
        self.benchmark_return = []
        self.blotter = []
        self.buy_value = []
        self.sell_value = []
        self.initial_value = sim_params.portfolio.cash
        trading_days = get_trading_days(sim_params.start, sim_params.end)
        for s, a in sim_params.portfolio.secpos.items():
            if len(trading_days) > 0:
                p = data['preClosePrice'].at[trading_days[0].strftime('%Y-%m-%d'), s]
                self.initial_value += p * a
        assert isinstance(universe_service, UniverseService)
        self.len_universe = len(universe_service.view(with_init_universe=True))

    def _update_trade_value(self, account, data):
        """
        根据回测的具体情况更新某个交易日的和持仓相关的报告数据

        Args:
            account (Account): 账户对象实例
            data (data): 缓存数据
        """
        initial_position = dict([(k, {'amount': v}) for (k, v) in account.sim_params.security_base.iteritems()])
        yesterday_positions = self.security_position[-2] if len(self.security_position) > 1 else initial_position
        today_positions = self.security_position[-1]
        for s, v in yesterday_positions.iteritems():
            q_change = today_positions.get(s, {'amount': 0})['amount'] - v['amount']
            if q_change > 0:
                self.buy_value.append(q_change * data.at[s, 'openPrice'])
            else:
                self.sell_value.append(- q_change * data.at[s, 'openPrice'])
        for s, v in today_positions.iteritems():
            if s not in yesterday_positions:
                self.buy_value.append(v['amount'] * data.at[s, 'openPrice'])

    def update(self, context, account_name):
        """
        更新相应指标的数据

        Args:
            context (context): Environment 对象
            account_name (str): 账户名称
        """
        data = context.registered_accounts[account_name].broker.daily_data[context.current_date.strftime('%Y-%m-%d')]
        record = context.registered_accounts[account_name].to_record()
        self.tradeDate.append(record["current_date"])
        self.blotter.append(record["blotter"])
        self.cash.append(record["position"].cash)
        self.portfolio_value.append(record["position"].evaluate(data))
        self.security_position.append(record["position"].show())
        major_benchmark = context.sim_params.major_benchmark
        self.benchmark_return.append(
            data.at[major_benchmark, 'closePrice'] / data.at[major_benchmark, 'preClosePrice'] - 1
            if data.at[major_benchmark, 'preClosePrice'] != 0.0 else 0.0)
        self._update_trade_value(context, data)

        for k in record:
            if k not in (self.std_keys + self.sup_keys + self.rec_keys):
                self.sup_keys.append(k)
                setattr(self, k, [])

        for k in self.sup_keys:
            getattr(self, k).append(record[k])

    def output(self):
        """
        输出成pandas.DataFrame格式

        Returns:
            DataFrame: 回测记录
        """
        output_dict = {k: getattr(self, k) for k in (self.std_keys + self.sup_keys)}
        output_frame = pd.DataFrame(output_dict).loc[:, (self.std_keys + self.sup_keys)]
        output_frame.index = output_frame.tradeDate
        return output_frame


class FuturesReport(object):
    """
    期货回测记录，包含如下属性

    * self.trade_date : 记录日期列表
    * self.futures_blotter : 记录交易指令列表
    * self.futures_cash : 记录现金列表
    * self.future_position : 记录期货持仓
    * self.futures_position_detail : 记录期货持仓明细
    * self.portfolio_value : 记录投资组合价值
    """
    def __init__(self, initial_value=None):
        # 输出的字段
        self.keys = ['trade_date', 'futures_cash', 'futures_position', 'futures_blotter',
                     'portfolio_value']

        self.trade_date = []
        self.futures_blotter = []
        self.futures_cash = []
        self.futures_position = []
        self.futures_position_detail = []
        self.portfolio_value = []
        self.trades = []
        self.benchmark_return = []
        self.initial_value = initial_value if initial_value else 10000000

    def output(self):
        """
        输出成pandas.DataFrame格式

        Returns:
            DataFrame: 回测记录
        """
        for key, value in enumerate(self.portfolio_value):
            if value <= 0:
                break
        try:
            if key < len(self.portfolio_value) - 1:
                self.portfolio_value = self.portfolio_value[:key] + [0]*(len(self.portfolio_value)-key)
                self.futures_cash = self.futures_cash[:key] + [0]*(len(self.futures_cash)-key)
                self.futures_blotter = self.futures_blotter[:key] + [[]]*(len(self.futures_blotter)-key)
                self.futures_position = self.futures_position[:key] + [{}]*(len(self.futures_position)-key)
                self.trades = self.trades[:key] + [[]]*(len(self.trades)-key)
        except:
            pass

        df = pd.DataFrame({
            'tradeDate': self.trade_date,
            'futures_blotter': self.futures_blotter,
            'futures_cash': self.futures_cash,
            'futures_position': self.futures_position,
            'portfolio_value': self.portfolio_value,
            'futures_trades': self.trades,
            'benchmark_return': self.benchmark_return
            })
        df.index = self.trade_date
        return df

    def update(self, context, account_name):
        """
        更新相应指标的数据

        Args:
            context (context): Environment 对象
            account_name (str): 账户名称
        """
        account = context.get_account(account_name)
        self.trade_date.append(account.clock.current_date)
        self.futures_blotter.append(deepcopy(account.broker.blotter.to_list()))
        self.futures_cash.append(account.broker.portfolio.settle_cash)
        self.futures_position.append(account.broker.portfolio.settle_position)
        self.portfolio_value.append(account.broker.portfolio.pre_portfolio_value)
        self.trades.append(deepcopy(account.get_trades()))
        data = account.broker.daily_data[context.current_date.strftime('%Y-%m-%d')]
        major_benchmark = context.sim_params.major_benchmark
        self.benchmark_return.append(data.loc[major_benchmark, 'closePrice']
                                     / data.loc[major_benchmark, 'preClosePrice'] - 1)


class IndexReport(object):

    """
    指数账户回测记录，包含如下属性

    * self.trade_date : 记录日期列表
    * self.index_blotter : 记录交易指令列表
    * self.index_cash : 记录现金列表
    * self.index_position : 记录期货持仓
    * self.index_trades : 记录期货成交明细
    * self.benchmark_return : 记录 Benchmark 收益
    * self.portfolio_value : 记录投资组合价值
    """

    def __init__(self, account):
        self.account = account
        self.trade_date = list()
        self.index_cash = list()
        self.index_blotter = list()
        self.index_position = list()
        self.index_trades = list()
        self.portfolio_value = list()
        self.benchmark_return = list()

    def output(self):
        """
        输出成pandas.DataFrame格式

        Returns:
            DataFrame: 回测记录
        """
        output_dict = {
            'tradeDate': self.trade_date,
            'index_blotter': self.index_blotter,
            'index_cash': self.index_cash,
            'index_position': self.index_position,
            'index_trades': self.index_trades,
            'benchmark_return': self.benchmark_return,
            'portfolio_value': self.portfolio_value
        }
        return pd.DataFrame(output_dict, index=self.trade_date)

    def update(self, context, account_name):
        """
        更新相应指标的数据

        Args:
            context (context): Environment 对象
            account_name (str): 账户名称
        """
        self.trade_date.append(self.account.clock.current_date)
        self.index_cash.append(self.account.cash)
        self.index_blotter.append(self.account.broker.blotter.to_list())
        self.index_position.append(
            deepcopy({key: position.__dict__() for key, position in self.account.get_positions().iteritems()}))
        self.index_trades.append(copy(self.account.broker.trades))
        self.portfolio_value.append(self.account.portfolio_value)
        data = context.registered_accounts[account_name].broker.daily_data[context.current_date.strftime('%Y-%m-%d')]
        major_benchmark = context.sim_params.major_benchmark
        self.benchmark_return.append(data.loc[major_benchmark, 'closePrice']
                                     / data.loc[major_benchmark, 'preClosePrice'] - 1)


class OTCFundReport(object):

    """
    场外基金回测记录，包含如下属性

    * self.trade_date : 记录日期列表
    * self.otc_fund_blotter : 记录交易指令列表
    * self.otc_fund_cash : 记录现金列表
    * self.otc_fund_position : 记录期货持仓
    * self.otc_fund_trades : 记录期货成交明细
    * self.benchmark_return : 记录 Benchmark 收益
    * self.portfolio_value : 记录投资组合价值
    """

    def __init__(self, account):
        self.account = account
        self.trade_date = list()
        self.otc_fund_cash = list()
        self.otc_fund_blotter = list()
        self.otc_fund_position = list()
        self.otc_fund_trades = list()
        self.portfolio_value = list()
        self.benchmark_return = list()

    def output(self):
        """
        输出成pandas.DataFrame格式

        Returns:
            DataFrame: 回测记录
        """
        output_dict = {
            'tradeDate': self.trade_date,
            'otc_fund_blotter': self.otc_fund_blotter,
            'otc_fund_cash': self.otc_fund_cash,
            'otc_fund_position': self.otc_fund_position,
            'otc_fund_trades': self.otc_fund_trades,
            'benchmark_return': self.benchmark_return,
            'portfolio_value': self.portfolio_value
        }
        return pd.DataFrame(output_dict, index=self.trade_date)

    def update(self, context, account_name):
        """
        更新相应指标的数据

        Args:
            context (context): Environment 对象
            account_name (str): 账户名称
        """
        self.trade_date.append(self.account.clock.current_date)
        self.otc_fund_cash.append(self.account.cash)
        self.otc_fund_blotter.append(self.account.broker.blotter.to_list())
        self.otc_fund_position.append(
            deepcopy({key: position.__dict__ for key, position in self.account.get_positions().iteritems()}))
        self.otc_fund_trades.append(self.account.broker.trades)
        self.portfolio_value.append(self.account.portfolio_value)
        data = context.registered_accounts[account_name].broker.daily_data[context.current_date.strftime('%Y-%m-%d')]
        major_benchmark = context.sim_params.major_benchmark
        self.benchmark_return.append(data.loc[major_benchmark, 'closePrice']
                                     / data.loc[major_benchmark, 'preClosePrice'] - 1)


class Reporter(object):

    risk_free_rates = None

    def __init__(self, context):
        self.initial_value = context.initial_value
        self.universe_service = context.universe_service
        self.registered_accounts = context.registered_accounts
        self.registered_accounts_params = context.registered_accounts_params
        self.context = context
        self.initial_market_data = None
        self._inited = False
        self.reports = None

    def cal_init_market_data(self):
        if not self._inited:
            self.initial_market_data = self.context.market_service.slice(
                list(self.context.asset_service.filter_symbols(asset_type=AssetType.EQUITIES)), ['preClosePrice'],
                end_date=self.context.calendar_service.trading_days[0], time_range=2)
            self.reports = self._init_reports()
            self._inited = True

    def _init_reports(self):
        """
        初始化账户
        """
        reports = dict()
        for account_name, account in self.registered_accounts.iteritems():
            if isinstance(account, StockAccount):
                reports[account_name] = StocksReport(
                    data=self.initial_market_data,
                    sim_params=self.registered_accounts_params[account_name],
                    universe_service=self.universe_service)
            elif isinstance(account, FuturesAccount):
                reports[account_name] = FuturesReport(
                    initial_value=self.registered_accounts_params[account_name])
            elif isinstance(account, OTCFundAccount):
                reports[account_name] = OTCFundReport(account)
            elif isinstance(account, IndexAccount):
                reports[account_name] = IndexReport(account)
        return reports

    def bt_collections(self):
        """
        Returns:
            dict of DataFrame: 分账户 bt
        """
        return {account_name: report.output() for account_name, report in self.reports.iteritems()}

    def bt(self):
        """
        Returns:
            DataFrame: 汇总 bt
        """
        bt_collections = self.bt_collections()
        if len(bt_collections) == 0:
            raise ValueError('Backtest report is empty, please check the date range and history window!')
        elif len(bt_collections) == 1:
            temp_bt = bt_collections.values()[0]
            if temp_bt.columns.tolist().count('cash'):
                temp_bt.index = range(temp_bt.shape[0])
            aggregate_bt = temp_bt
        else:
            aggregate_bt = smart_concat(bt_collections.itervalues(), axis=1)
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

    def perf(self):
        """
        Returns:
            dict: 用户的行为表现
        """
        bt = self.bt()
        initial_value = self.initial_value
        universe_service = self.universe_service
        keys = ['annualized_return', 'volatility', 'returns', 'cumulative_values', 'cumulative_returns',
                'benchmark_annualized_return', 'benchmark_volatility', 'benchmark_returns',
                'benchmark_cumulative_values', 'benchmark_cumulative_returns', 'treasury_return', 'alpha', 'beta',
                'excess_return', 'sharpe', 'information_ratio',
                'information_coefficient', 'max_drawdown', 'turnover_rate']

        perf = {key: None for key in keys}
        trading_days = bt['tradeDate']
        st_returns = get_return([initial_value] + bt['portfolio_value'].tolist())
        bm_returns = bt['benchmark_return'].tolist()
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
        rf = self._get_risk_free_rate(bt.iat[0, bt.columns.get_loc('tradeDate')])
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
            perf['information_coefficient'] = perf['information_ratio'] / (len_universe * len(bt)) ** 0.5

        perf['max_drawdown'] = get_max_drawdown(c_st_values)

        turnover_rate_candidates = []
        for account_name, account in self.registered_accounts.iteritems():
            if isinstance(account, StockAccount):
                turnover_rate_candidates.append(
                    get_turnover_rate(sum(self.reports[account_name].buy_value),
                                      sum(self.reports[account_name].sell_value),
                                      np.array(self.reports[account_name].portfolio_value).mean()))
            else:
                turnover_rate_candidates.append(0)
        perf['turnover_rate'] = max(turnover_rate_candidates)
        return perf

    def update_report(self, context):
        for account_name, account in self.registered_accounts.iteritems():
            self.reports[account_name].update(context, account_name)

    def _get_risk_free_rate(self, date):
        return 0.035