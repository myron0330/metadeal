# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Database API.
#     Desc: define general database API for the service.
# **********************************************************************************#
import bisect
import DataAPI
import pandas as pd
from datetime import datetime, timedelta


def load_daily_bar(start, end, symbols):
    """
    Load futures daily bar daa
    """
    pass


def _normalize_date(date):
    """
    将日期标准化为datetime.datetime格式

    Args:
        date (datetime.datetime or str): datetime

    Returns:
        datetime.datetime: 标准化之后的日期

    Examples:
        >> normalize_date(datetime(2015, 1, 1))
        >> normalize_date('2015-01-01')
        >> normalize_date('20150101')
    """
    date = pd.Timestamp(date)
    return datetime(date.year, date.month, date.day)


def get_trading_days(start, end):
    """
    Get trading days.
    Args:
        start(string or datetime): start date
        end(string or datetime): end date

    Returns:
        list of datetime: trading days list

    """
    return DataAPI.get_trading_days(start=start, end=end)


def get_direct_trading_day(date, step, forward):
    """
    Get direct trading day.

    Args:
        date(string or datetime):
        step:
        forward:

    Returns:
    """
    if step > 50:
        raise Exception('step can only be less than 20.')
    date = _normalize_date(date)
    start_date = date - timedelta(100)
    end_date = date + timedelta(100)
    target_trading_days = DataAPI.get_trading_days(start=start_date,
                                                   end=end_date)
    date_index = bisect.bisect_left(target_trading_days, date)
    target_index = date_index + (1 if forward else -1) * step
    return target_trading_days[target_index]


def load_daily_futures_data(*args, **kwargs):
    """
    Load daily futures data.
    Args:
        *args:
        **kwargs:

    Returns:

    """
    pass


def load_minute_futures_data(*args, **kwargs):
    """

    Args:
        *args:
        **kwargs:

    Returns:

    """
    pass


def get_futures_base_info(symbols=None):
    """
    Get futures base info.

    Args:
        symbols(list): basic future symbols
    """
    if symbols:
        data = DataAPI.FutuGet(ticker=symbols)
    else:
        data = DataAPI.FutuGet()
    rename_dict = {
        'ticker': 'symbol'
    }
    data.rename(columns=rename_dict, inplace=True)
    data.symbol = data.symbol.apply(lambda x: x.upper())
    return data


def get_futures_main_contract(contract_objects=None, trading_days=None, start=None, end=None):
    """
    Get futures main contract

    Args:
        contract_objects(list): continuous symbols
        trading_days(list): trading days
        start(string or datetime.datetime): start date
        end(string or datetime.datetime): end date
    """
    start = start or trading_days[0]
    end = end or trading_days[-1]
    data = DataAPI.MktMFutdGet(mainCon=1, startDate=start, endDate=end, pandas="1")
    data.ticker = data.ticker.apply(lambda x: x.upper())
    frame = data.pivot(index='tradeDate', columns='contractObject', values='ticker')
    if contract_objects:
        return frame[contract_objects]
    return frame


if __name__ == '__main__':
    print get_trading_days(datetime(2015, 1, 1), datetime(2015, 2, 1))
    print get_direct_trading_day(datetime(2015, 1, 1), step=0, forward=True)
    print get_direct_trading_day(datetime(2015, 1, 1), step=1, forward=True)
    print get_direct_trading_day(datetime(2015, 1, 1), step=1, forward=False)
    print get_futures_base_info(['RB1810'])
    print get_futures_main_contract(contract_objects=['RB', 'AG'], start='20180401', end='20180502')
