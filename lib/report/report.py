# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Report file
# **********************************************************************************#
import pandas as pd
from collections import OrderedDict
from .. core.objects import ValueObject
from .. core.enums import AccountType
from .. utils.error_utils import Errors


def choose_report(account_type):
    """
    Choose report object by account type

    Args:
        account_type(string): account type
    """
    if account_type == AccountType.security:
        report_obj = SecurityReport
    elif account_type == AccountType.futures:
        report_obj = FuturesReport
    elif account_type == AccountType.index:
        report_obj = IndexReport
    elif account_type == AccountType.otc_fund:
        report_obj = OTCFundReport
    else:
        raise Errors.INVALID_ACCOUNT_TYPE
    return report_obj


class Report(ValueObject):

    __slots__ = [
        'trade_dates',
        'cash',
        'orders',
        'positions',
        'trades',
        'portfolio_value',
        'benchmark_return',
    ]

    def __init__(self, trade_dates=None, cash=None, orders=None,
                 positions=None, trades=None, portfolio_value=None,
                 benchmark_return=None):
        self.trade_dates = trade_dates
        self.cash = cash
        self.orders = orders
        self.positions = positions
        self.trades = trades
        self.portfolio_value = portfolio_value
        self.benchmark_return = benchmark_return


class SecurityReport(Report):
    """
    Security report
    """
    def output(self, users_records=None):
        """
        Args:
            users_records(dict): user records

        Returns:
            DataFrame: bt frame
        """
        frame_data = OrderedDict([
            ('tradeDate', self.trade_dates),
            ('cash', self.cash),
            ('blotter', self.orders),
            ('security_position', self.positions),
            ('portfolio_value', self.portfolio_value),
            ('benchmark_return', self.benchmark_return)
        ])
        frame = pd.DataFrame(frame_data).loc[:, frame_data.keys()]
        frame.tradeDate = frame.tradeDate.apply(lambda x: x.strftime('%Y-%m-%d'))
        frame.index = frame.tradeDate
        return frame


class FuturesReport(Report):
    """
    Futures report
    """
    def output(self, users_records=None):
        """
        Args:
            users_records(dict): user records

        Returns:
            DataFrame: bt frame
        """
        frame_data = OrderedDict([
            ('tradeDate', self.trade_dates),
            ('futures_cash', self.cash),
            ('futures_blotter', self.orders),
            ('futures_position', self.positions),
            ('futures_trades', self.trades),
            ('portfolio_value', self.portfolio_value),
            ('benchmark_return', self.benchmark_return)
        ])
        frame = pd.DataFrame(frame_data).loc[:, frame_data.keys()]
        frame.tradeDate = frame.tradeDate.apply(lambda x: x.strftime('%Y-%m-%d'))
        frame.index = frame.tradeDate
        return frame


class IndexReport(Report):
    """
    Index report
    """

    def output(self, users_records=None):
        """
        Args:
            users_records(dict): user records

        Returns:
            DataFrame: bt frame
        """
        frame_data = OrderedDict([
            ('tradeDate', self.trade_dates),
            ('index_cash', self.cash),
            ('index_blotter', self.orders),
            ('index_position', self.positions),
            ('index_trades', self.trades),
            ('portfolio_value', self.portfolio_value),
            ('benchmark_return', self.benchmark_return)
        ])
        frame = pd.DataFrame(frame_data).loc[:, frame_data.keys()]
        frame.tradeDate = frame.tradeDate.apply(lambda x: x.strftime('%Y-%m-%d'))
        frame.index = frame.tradeDate
        return frame


class OTCFundReport(Report):
    """
    OTCFund report
    """

    def output(self, users_records=None):
        """
        Args:
            users_records(dict): user records

        Returns:
            DataFrame: bt frame
        """
        frame_data = OrderedDict([
            ('tradeDate', self.trade_dates),
            ('otc_fund_cash', self.cash),
            ('otc_fund_blotter', self.orders),
            ('otc_fund_position', self.positions),
            ('otc_fund_trades', self.trades),
            ('portfolio_value', self.portfolio_value),
            ('benchmark_return', self.benchmark_return)
        ])
        frame = pd.DataFrame(frame_data).loc[:, frame_data.keys()]
        frame.tradeDate = frame.tradeDate.apply(lambda x: x.strftime('%Y-%m-%d'))
        frame.index = frame.tradeDate
        return frame
