# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Constants file
# **********************************************************************************#
from datetime import datetime


DEFAULT_CAPITAL_BASE = 1e5
DEFAULT_PORTFOLIO_VALUE_BASE = DEFAULT_CAPITAL_BASE
DEFAULT_USER_NAME = 'myron'
DEFAULT_ACCOUNT_NAME = 'fantasy_account'
DEFAULT_BENCHMARK = 'RBM0'
DEFAULT_FILLED_AMOUNT = 0
DEFAULT_FILLED_TIME = None
DEFAULT_TRANSACT_PRICE = 0

DEFAULT_KEYWORDS = {
    'start': datetime.today().strftime('%Y-%m-%d'),
    'end': datetime.today().strftime('%Y-%m-%d'),
    'benchmark': 'HS300',
    'universe': ['RBM0'],
    'capital_base': float(1e5),
    'initialize': (lambda x: None),
    'handle_data': (lambda x: None),
    'post_trading_day': (lambda x: None),
    'security_base': dict(),
    'security_cost': dict(),
    'refresh_rate': 1,
    'freq': 'd',
    'max_history_window': (30, 241),
    'accounts': dict(),
    'position_base': dict(),
    'cost_base': dict()
}
