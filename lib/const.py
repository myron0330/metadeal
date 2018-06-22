# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Constants file
# **********************************************************************************#
import re
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
    'benchmark': 'RBM0',
    'universe': ['RBM0'],
    'capital_base': float(1e5),
    'initialize': (lambda x: None),
    'handle_data': (lambda x: None),
    'post_trading_day': (lambda x: None),
    'refresh_rate': 1,
    'freq': 'd',
    'max_history_window': (30, 241),
    'accounts': dict(),
    'position_base': dict(),
    'cost_base': dict()
}

EARLIEST_DATE = datetime(2006, 1, 1)

SYMBOL_PATTERN_STOCK = '[036]\d{5}\.(XSHE|XSHG)'
SYMBOL_PATTERN_BASE_FUTURES = '[A-Z]{1,2}\d{3,4}$'
SYMBOL_PATTERN_FUTURE_PRODUCT = '^[A-Z]{1,2}'
SYMBOL_PATTERN_CONTINUOUS_FUTURES = '[A-Z]{1,2}(M0|M1|N0|N1|P0|P1|L0|L1|L3|L6)$'
SYMBOL_PATTERN_INDEX = '(\d{6}.ZICN|000\d{3}.XSHG|399\d{3}.XSHE|[A-Z0-9]{2,}\.(?!(XSHG|XSHE|OFCN)))'
SYMBOL_PATTERN_HS_INDEX = '(\d{6}.ZICN|000\d{3}.XSHG|399\d{3}.XSHE)'
SYMBOL_PATTERN_FUND = '([15]\d{5}\.(XSHE|XSHG)|\d{6}\.OFCN)\d{0,1}'
SYMBOL_PATTERN_EXCHANGE_FUND = '[15]\d{5}\.(XSHE|XSHG)\d{0,1}'
SYMBOL_PATTERN_OTC_FUND = '\d{6}\.OFCN\d{0,1}'
SYMBOL_PATTERN_OPTION = '^(510050[CP]|(m\d{4}-[CP]-|SR\d{3}[CP]))\d{4}'
SYMBOL_PATTERN_NH_FUTURE_INDEX = '[\w]+.NHCI'
ID_INDEX_MAP = {'000016.ZICN': 'SH50', '000010.ZICN': 'SH180', '000300.ZICN': 'HS300', '000905.ZICN': 'ZZ500'}
STATIC_SYMBOL_MAP = {'A': u'全A股', 'ZXB': u'中小板', 'CYB': u'创业板'}
STATIC_MAP_FROM_SYMBOL = {'000002.ZICN': 'A', '399101.ZICN': 'ZXB', '399102.ZICN': 'CYB'}
STATIC_SYMBOL_INDEX = {'A': '000002.ZICN', 'ZXB': '399101.ZICN', 'CYB': '399102.ZICN'}

FUTURES_EXCHANGE_MAP = {'Futures.CHINA': 'all',
                        'Futures.ZhongJinSuo': 'CCFX',
                        'Futures.ShangQiSuo': 'XSGE',
                        'Futures.DaShangSuo': 'XDCE',
                        'Futures.ZhengShangSuo': 'XZCE'}

STOCK_PATTERN = re.compile(SYMBOL_PATTERN_STOCK)
BASE_FUTURES_PATTERN = re.compile(SYMBOL_PATTERN_BASE_FUTURES)
CONTINUOUS_FUTURES_PATTERN = re.compile(SYMBOL_PATTERN_CONTINUOUS_FUTURES)
INDEX_PATTERN = re.compile(SYMBOL_PATTERN_INDEX)
HS_INDEX_PATTERN = re.compile(SYMBOL_PATTERN_HS_INDEX)
NH_FUTURE_INDEX_PATTERN = re.compile(SYMBOL_PATTERN_NH_FUTURE_INDEX)
FUND_PATTERN = re.compile(SYMBOL_PATTERN_FUND)
XZCE_FUTURES_PATTERN = re.compile('([A-Z]{2})(\d{3})$')
OPTION_PATTERN = re.compile(SYMBOL_PATTERN_OPTION)

FUTURES_DAILY_FIELDS = ['tradeDate', 'openPrice', 'highPrice', 'lowPrice', 'closePrice', 'settlementPrice',
                        'volume', 'openInterest', 'preSettlementPrice', 'turnoverVol', 'turnoverValue']
FUTURES_MINUTE_FIELDS = ['tradeDate', 'clearingDate', 'barTime', 'openPrice', 'highPrice', 'lowPrice',
                         'closePrice', 'volume', 'tradeTime', 'turnoverVol', 'turnoverValue', 'openInterest']


ADJ_FACTOR = 'default_adj_factor'
MAX_CACHE_DAILY_PERIODS = 10
TRADE_ESSENTIAL_DAILY_BAR_FIELDS = ['preClosePrice', 'openPrice', 'closePrice', 'highPrice',
                                    'lowPrice', 'turnoverVol', 'volume', 'preSettlementPrice',
                                    'settlementPrice', 'openInterest', 'turnoverValue',
                                    'nav', 'accumNav', 'adjustNav', 'adjFactor']
TRADE_ESSENTIAL_MINUTE_BAR_FIELDS = ['openPrice', 'closePrice', 'highPrice', 'lowPrice',
                                     'turnoverVol', 'barTime', 'tradeTime']
HISTORY_ESSENTIAL_MINUTE_BAR_FIELDS = ['openPrice', 'closePrice', 'highPrice', 'lowPrice',
                                       'turnoverVol', 'turnoverValue', 'barTime', 'tradeTime']
REAL_TIME_MINUTE_BAR_FIELDS = ['barTime', 'closePrice', 'highPrice', 'lowPrice', 'openPrice',
                               'totalValue', 'totalVolume']
