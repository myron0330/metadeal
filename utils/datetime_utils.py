# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
#   Author: Myron
# **********************************************************************************#
import time
import pandas as pd
from datetime import datetime, timedelta
# from .. data.database_api import get_direct_trading_day, get_trading_days


def get_trading_days(start, end):
    """
    Get trading days by start and end.
    Args:
        start(string or datetime.datetime): start time
        end(string or datetime.datetime): end time
    """
    start, end = normalize_date(start), normalize_date(end)
    trading_days = pd.date_range(start=start, end=end, freq='B')
    return trading_days


def normalize_date(date):
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


def get_current_date():
    """
    Get today
    """
    return normalize_date(datetime.today())


def get_previous_date(date=None):
    """
    Get previous date
    """
    current_date = date or get_current_date()
    return current_date - timedelta(1)


def get_next_date(date=None):
    """
    Get next date
    """
    current_date = date or get_current_date()
    return current_date + timedelta(1)


def get_previous_trading_date(date=None):
    """
    Get previous date
    """
    current_date = date or get_current_date()
    return get_direct_trading_day(current_date, step=1, forward=False)


def get_next_trading_date(date=None):
    """
    Get next trading date
    """
    current_date = date or get_current_date()
    return get_direct_trading_day(current_date, step=1, forward=True)


def get_upcoming_trading_date(date=None, is_trading_day=None):
    """
    Get upcoming trading date

    Args:
        date(datetime.datetime): datetime timestamp
        is_trading_day(boolean): optional, whether the date is trading day
    """
    current_date = date or get_current_date()
    flag = is_trading_day \
        if is_trading_day is not None else bool(get_trading_days(start=current_date, end=current_date))
    step = 1 if flag else 0
    return get_direct_trading_day(current_date, step=step, forward=True)


def get_current_minute():
    """
    Get current minute
    """
    return datetime.now().strftime('%H:%M')


def get_previous_minute():
    """
    Get previous minute
    """
    previous_minute = datetime.now() - timedelta(seconds=60)
    return previous_minute.strftime('%H:%M')


def get_current_time_stamp():
    """
    Current time stamp
    """
    return time.time() * 1000


def get_latest_trading_date(date):
    """
    Get latest trading date
    """
    trading_day = get_trading_days(start=date, end=date)
    step = 0 if bool(trading_day) else 1
    return get_direct_trading_day(date, step=step, forward=False)


def date_to_timestamp(date):
    """
    Transfer date to timestamp.

    Args:
        date(datetime or string): datetime or %Y%m%d
    """
    date = normalize_date(date).strftime('%Y%m%d')
    return time.mktime(time.strptime(date, '%Y%m%d')) * 1000


def timestamp_to_date(timestamp):
    """
    Transfer timestamp to date.

    Args:
        timestamp(int or float): timestamp
    """
    return time.strftime('%Y%m%d', time.localtime(timestamp / 1000))
