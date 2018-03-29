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
    if step > 10:
        raise Exception('step can only be less than 20.')
    date = _normalize_date(date)
    start_date = date - timedelta(30)
    end_date = date + timedelta(30)
    target_trading_days = DataAPI.get_trading_days(start=start_date,
                                                   end=end_date)
    date_index = bisect.bisect_left(target_trading_days, date)
    target_index = date_index + (1 if forward else -1) * step
    return target_trading_days[target_index]


if __name__ == '__main__':
    print get_trading_days(datetime(2015, 1, 1), datetime(2015, 2, 1))
    print get_direct_trading_day(datetime(2015, 1, 1), step=0, forward=True)
    print get_direct_trading_day(datetime(2015, 1, 1), step=1, forward=True)
    print get_direct_trading_day(datetime(2015, 1, 1), step=1, forward=False)
