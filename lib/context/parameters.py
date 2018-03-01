# -*- coding: utf-8 -*-

"""
env.py

backtest environment classes

@author: yudi.wu
"""

from datetime import datetime
from .. data_loader.cache_api import is_updated_today
from .. universe.universe import Universe
from .. trade.cost import Commission, Slippage
from .. utils.data_utils import check_secids
from .. utils.datetime_utils import (
    not_valid_date,
    is_valid_dateinput,
    get_trading_days,
    get_minute_bars,
    is_valid_min_start_date,
    get_end_date
)
from .. utils.error_utils import (
    BacktestInputError,
    BacktestInputMessage
)
from .. const import PRESET_KEYARGS


BENCHMARKMAP = {'SHCI':  '000001.ZICN',
                'SH50':  '000016.ZICN',
                'SH180': '000010.ZICN',
                'HS300': '000300.ZICN',
                'ZZ500': '000905.ZICN'}


class SimulationParameters(object):
    """
    回测参数，所有预先需要加载的数据需要能够通过SimulationParameter确定，包含如下属性

    * self._start：回测开始时间
    * self._end：回测结束时间
    * self._trading_days：回测期间的所有交易日列表
    * self._minute_bars：分钟线列表
    * self._benchmarks: 参照标准列表
    * self._major_benchmark: 主要参照标准
    * self._universe：证券池
    * self._position：初始资金
    """

    def __init__(
        self,
        start=PRESET_KEYARGS['start'],
        end=PRESET_KEYARGS['end'],
        benchmark=PRESET_KEYARGS['benchmark'],
        universe=PRESET_KEYARGS['universe'],
        capital_base=PRESET_KEYARGS['capital_base'],
        security_base=PRESET_KEYARGS['security_base'],
        security_cost=PRESET_KEYARGS['security_cost'],
        freq=PRESET_KEYARGS['freq'],
        refresh_rate=PRESET_KEYARGS['refresh_rate'],
        commission=Commission(),
        slippage=Slippage(),
        max_history_window=PRESET_KEYARGS['max_history_window'],
        isbacktest=1,
        margin_rate=None,
        accounts=None,
        position_base_by_accounts=None,
        cost_base_by_accounts=None,
        capital_base_by_accounts=None,
        threaded=False
    ):
        """
        初始化，会解析benchmark和universe

        Args:
            start (datetime or str): 回测起始日期，支持datetime和str两种，str形式的日期可以是任何可被pd.tseries.tools.normalize_date处理的形式
            end (datetime or str): 回测结束日期，支持datetime和str两种，str形式的日期可以是任何可被pd.tseries.tools.normalize_date处理的形式
            benchmark (str or list): 参考标准，支持预设指数名（如'HS300'）、指数代码（如'000300.ZICN'）、其他证券代码（如'600000.XSHG'）或者这些名字/代码的列表
            universe (Universe or list): 证券池，支持自定义的证券代码列表或者使用Universe定义的动态证券池
            capital_base (float): 初始资金
            security_base (dict, str: int): 初始仓位，键为证券代码，值为证券数量
            security_cost (dict, str: float): 初始持仓成本，键为证券代码，值为证券成本
            freq (str): 策略调仓频率
            max_history_window(int or tuple): 预获取最大时间
        """
        if not_valid_date(start):
            msg = BacktestInputMessage.BAD_BEGIN_END_DATE.format(start)
            raise BacktestInputError(msg)
        if not_valid_date(end):
            msg = BacktestInputMessage.BAD_BEGIN_END_DATE.format(end)
            raise BacktestInputError(msg)
        self._start, self._end = is_valid_dateinput(start, end)
        is_valid_min_start_date(freq, self.start)
        self._refresh_rate = refresh_rate
        if freq not in 'dm':
            msg = BacktestInputMessage.INVALID_FREQ.format(freq)
            raise BacktestInputError(msg)
        if isinstance(refresh_rate, (tuple, list)):
            self._refresh_rate_d, self._refresh_rate_m = refresh_rate
        else:
            if freq == 'd':
                self._refresh_rate_d, self._refresh_rate_m = refresh_rate, 1
            else:
                self._refresh_rate_d, self._refresh_rate_m = 1, refresh_rate
        today = get_end_date()
        if isbacktest == 1:
            if is_updated_today():
                if self._end > today:
                    raise BacktestInputError('end date must not be later than {}!'.format(today.strftime("%Y-%m-%d")))
            else:
                if self._end >= today:
                    raise BacktestInputError('end date must be earlier than {}!'.format(today.strftime("%Y-%m-%d")))
        self.is_backtest = isbacktest == 1
        self._trading_days = get_trading_days(start, end, validation=False)
        self._minute_bars = get_minute_bars()
        if not self._trading_days:
            raise BacktestInputError('Exception in "SimulationParameters": No trading days between {} and {}, '
                                     'please verify your start and end date.'
                                     ''.format(self._start.strftime("%Y-%m-%d"), self._end.strftime("%Y-%m-%d")))
        # input check for Position
        if not isinstance(capital_base, (float, int, long)):
            raise BacktestInputError('capital_base must be integer or float!')
        if not isinstance(security_base, dict):
            raise BacktestInputError('security_base must be dict!')
        if not isinstance(security_cost, dict):
            raise BacktestInputError('security_cost must be dict!')

        check_secids(security_base.keys())
        check_secids(security_cost.keys())
        for v in security_base.values():
            if not isinstance(v, (float, int, long)):
                raise BacktestInputError('Exception in "SimulationParameters": '
                                         'Values of security_base must be integer or float!')
        for sec in security_cost:
            if sec not in security_base:
                raise BacktestInputError('Exception in "SimulationParameters": '
                                         'All security in security_cost must in security_base!')
            if not isinstance(security_cost[sec], (float, int, long)):
                raise BacktestInputError('Exception in "SimulationParameters": '
                                         'Values of security_cost must be integer or float!')
        self._freq = freq
        self.max_history_window_daily, self.max_history_window_minute = \
            self.parse_max_history_window(max_history_window)
        self._major_benchmark, self._benchmarks = self.parse_benchmark(benchmark)
        self.cash = capital_base
        self.slippage = slippage
        self.commission = commission
        self.margin_rate = margin_rate if margin_rate else dict()
        self.capital_base = capital_base
        self.security_base = security_base
        self.security_cost = security_cost
        self.position_base = self.security_base
        self.cost_base = self.security_cost
        self.portfolio = None
        self.accounts = self.parse_accounts(accounts)
        self.universe = self.parse_universe(universe)
        self.position_base_by_accounts = position_base_by_accounts or dict()
        self.cost_base_by_accounts = cost_base_by_accounts or dict()
        self.capital_base_by_accounts = capital_base_by_accounts or dict()
        self.threaded = threaded

    @property
    def freq(self):
        return self._freq

    @property
    def start(self):
        return self._start

    @property
    def end(self):
        return self._end

    @property
    def refresh_rate(self):
        return self._refresh_rate

    @property
    def trading_days(self):
        return self._trading_days

    @property
    def benchmarks(self):
        return self._benchmarks

    @property
    def major_benchmark(self):
        return self._major_benchmark

    @property
    def refresh_rate_d(self):
        return self._refresh_rate_d

    @property
    def refresh_rate_m(self):
        return self._refresh_rate_m

    @staticmethod
    def parse_max_history_window(max_history_window):
        max_history_window_daily, max_history_window_minute = None, None
        if isinstance(max_history_window, int):
            max_history_window_daily = max_history_window_minute = max_history_window
        elif isinstance(max_history_window, (list, tuple)):
            if len(max_history_window) == 1:
                max_history_window_daily = max_history_window_minute = max_history_window[0]
            elif len(max_history_window) > 1:
                max_history_window_daily = max_history_window[0]
                max_history_window_minute = max_history_window[1]
        return max_history_window_daily, max_history_window_minute

    @staticmethod
    def parse_benchmark(benchmark):
        """把参照标准映射成secID"""
        if isinstance(benchmark, str) or isinstance(benchmark, unicode):
            benchmarks = [BENCHMARKMAP.get(benchmark, benchmark)]
        elif isinstance(benchmark, (list, tuple)):
            benchmarks = [BENCHMARKMAP.get(bm, bm) for bm in benchmark]
        else:
            raise BacktestInputError("Invalid benchmark type!")

        check_secids(benchmarks, check_type='benchmark')
        major_benchmark, benchmarks = benchmarks[0], benchmarks
        return major_benchmark, benchmarks

    @staticmethod
    def parse_universe(universe):
        """构建Universe实例"""
        if isinstance(universe, list):
            generated_universe = Universe() + universe
            # generated_universe = Universe(*universe)
        elif isinstance(universe, Universe):
            generated_universe = universe
        else:
            raise BacktestInputError('Exception in "SimulationParameters": '
                                     'universe can only be "list", "Screener" or "Universe"!')
        return generated_universe

    @staticmethod
    def parse_accounts(accounts):
        """
        Parse accounts
        """
        return accounts

    def __repr__(self):
        repr_str = u"""
[Simulation Parameters]:
trading_days = {} ~ {},
freq = {},
benchmarks = {},
major benchmark = {},
universe = {},
""".format(self._trading_days[0].strftime("%Y-%m-%d"),
           self._trading_days[-1].strftime("%Y-%m-%d"),
           self._freq,
           self._benchmarks,
           self._major_benchmark,
           self.universe.__repr__())
        return repr_str.strip()
