# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Parameters File.
#   Author: Myron
# **********************************************************************************#
from utils.datetime_utils import normalize_date
from utils.error_utils import Errors
from .. data.universe_service import Universe
from .. trade.cost import Commission, Slippage
from .. const import DEFAULT_KEYWORDS


class SimulationParameters(object):
    """
    SimulationParameters
    """
    def __init__(
        self,
        start=DEFAULT_KEYWORDS['start'],
        end=DEFAULT_KEYWORDS['end'],
        benchmark=DEFAULT_KEYWORDS['benchmark'],
        universe=DEFAULT_KEYWORDS['universe'],
        capital_base=DEFAULT_KEYWORDS['capital_base'],
        position_base=DEFAULT_KEYWORDS['position_base'],
        cost_base=DEFAULT_KEYWORDS['cost_base'],
        freq=DEFAULT_KEYWORDS['freq'],
        refresh_rate=DEFAULT_KEYWORDS['refresh_rate'],
        commission=Commission(),
        slippage=Slippage(),
        max_history_window=DEFAULT_KEYWORDS['max_history_window'],
        margin_rate=None,
        accounts=None,
        position_base_by_accounts=None,
        cost_base_by_accounts=None,
        capital_base_by_accounts=None,
    ):
        self.start = normalize_date(start)
        self.end = normalize_date(end)
        self._refresh_rate = refresh_rate
        self._freq = freq
        self.max_history_window_daily, self.max_history_window_minute = \
            self.parse_max_history_window(max_history_window)
        self._major_benchmark, self._benchmarks = self.parse_benchmark(benchmark)
        self.cash = capital_base
        self.slippage = slippage
        self.commission = commission
        self.margin_rate = margin_rate if margin_rate else dict()
        self.capital_base = capital_base
        self.security_base = position_base
        self.security_cost = cost_base
        self.position_base = self.security_base
        self.cost_base = self.security_cost
        self.portfolio = None
        self.accounts = self.parse_accounts(accounts)
        self.universe = self.parse_universe(universe)
        self.position_base_by_accounts = position_base_by_accounts or dict()
        self.cost_base_by_accounts = cost_base_by_accounts or dict()
        self.capital_base_by_accounts = capital_base_by_accounts or dict()

    @property
    def freq(self):
        return self._freq

    @property
    def refresh_rate(self):
        return self._refresh_rate

    @property
    def benchmarks(self):
        return self._benchmarks

    @property
    def major_benchmark(self):
        return self._major_benchmark

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
        """
        Parse benchmark.

        Args:
            benchmark(string or list): benchmark

        Returns:
            (string, list): major benchmark, benchmark list
        """
        benchmarks = benchmark if isinstance(benchmark, (list, tuple)) else [benchmark]
        major_benchmark, benchmarks = benchmarks[0], benchmarks
        return major_benchmark, benchmarks

    @staticmethod
    def parse_universe(universe):
        """
        Parse universe.

        Args:
            universe(list or Universe): universe input

        Returns:
            Universe: universe instance
        """
        if isinstance(universe, list):
            generated_universe = Universe() + universe
        elif isinstance(universe, Universe):
            generated_universe = universe
        else:
            raise Errors.INVALID_UNIVERSE
        return generated_universe

    @staticmethod
    def parse_accounts(accounts):
        """
        Parse accounts

        Args:
            accounts(dict): accounts
        """
        return accounts

    def __repr__(self):
        repr_str = """
[Simulation Parameters]:
freq = {},
benchmarks = {},
major benchmark = {},
universe = {},
""".format(self._freq,
           self._benchmarks,
           self._major_benchmark,
           self.universe.__repr__())
        return repr_str.strip()
