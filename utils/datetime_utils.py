# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
#   Author: Myron
# **********************************************************************************#
import time
from datetime import datetime, timedelta
from lib.database.database_api import (
    get_trading_days,
    get_direct_trading_day,
    normalize_date,
    get_end_date
)


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


def is_trading_day(date):
    """
    Tell whether date is trading day.
    Args:
        date(datetime.datetime or string): date

    Returns:
        bool: whether date is trading day
    """
    current_date = normalize_date(date)
    trading_days = get_trading_days(start=current_date, end=current_date)
    return bool(trading_days)


def get_clearing_date_of(now=None):
    """
    Get clearing date of datetime.

    Args:
        now(datetime.datetime): now
    """
    now = now or datetime.now()
    if now.strftime('%H:%M') < '20:00' and is_trading_day(now):
        return normalize_date(now)
    return get_upcoming_trading_date(now)


__all__ = [
    'get_end_date',
    'get_trading_days',
    'get_previous_trading_date',
    'get_next_date',
    'get_current_date',
    'get_direct_trading_day',
    'get_next_trading_date',
    'get_current_minute',
    'get_current_time_stamp',
    'get_latest_trading_date',
    'get_previous_date',
    'get_previous_minute',
    'get_upcoming_trading_date',
    'date_to_timestamp',
    'timestamp_to_date',
    'get_clearing_date_of'
]
