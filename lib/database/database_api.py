# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Database API.
#     Desc: define general database API for the service.
# **********************************************************************************#
from configs import api_token
from api_client import Client
from utils.datetime_utils import normalize_date


client = Client()
client.init(api_token)


def load_daily_bar(start, end, symbols):
    """
    Load futures daily bar daa
    """
    pass


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
