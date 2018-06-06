# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Loader API.
#     Desc: define general loader API for history market data.
# **********************************************************************************#
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from . cache_api import (
    MktDailyFuturesDataGet,
    MktMinuteFuturesDataGet,
)
from .. const import (
    FUTURES_DAILY_FIELDS,
    FUTURES_MINUTE_FIELDS,
)

BATCH_DAILY_DATA_SIZE = 20000
MARKET_DATA_BATCH_COEFFICIENT = 1
BATCH_MINUTE_DATA_SIZE = 200
BATCH_FILTER_DAY_SIZE = 100
MAX_THREADS = 5
MIN_BATCH_SIZE = 8


def load_daily_futures_data(universe=None, trading_days=None, field=FUTURES_DAILY_FIELDS):
    """
    Load futures daily data concurrently.
    Available data: closePrice, highPrice, lowPrice, openPrice, turnoverVol, openInterest, settlementPrice

    Args:
        universe (list of str): futures universe list
        trading_days (list of datetime.datetime): trading days list
        field (list of string): needed fields
    Returns:
        dict of str=>DataFrame: key-->field，value-->DataFrame

    Examples:
        >> universe = ['IF1601']
        >> trading_days = get_trading_days()
        >> equity_data = load_daily_futures_data(universe, trading_days, ['closePrice'])
    """

    universe = list(universe)
    trading_days = sorted([dt.strftime("%Y%m%d") for dt in trading_days])
    trading_days_length = len(trading_days)
    universe_length = len(universe)

    batch_size = max(MIN_BATCH_SIZE, BATCH_DAILY_DATA_SIZE // trading_days_length)
    batches = [universe[i:min(i+batch_size, universe_length)] for i in range(0, universe_length, batch_size)]

    def _worker(index, batch):
        mkt_data = MktDailyFuturesDataGet(
            tickers=batch, start=trading_days[0], end=trading_days[-1])
        return index, mkt_data

    with ThreadPoolExecutor(MAX_THREADS) as pool:
        requests = [pool.submit(_worker, idx, bat) for (idx, bat) in enumerate(batches)]
        responses = {future.result()[0]: future.result()[1] for future in as_completed(requests)}

    data_all = {}
    for response in sorted(responses.items(), key=lambda x: x[0]):
        _, data = response
        data_all.update(data)
    data_all = {key: pd.DataFrame.from_dict(item).set_index('tradeDate', drop=False) for (key, item) in data_all.iteritems()}
    data_all = dict(pd.Panel.from_dict(data_all).swapaxes(0, 2))
    return data_all


def load_minute_futures_data(universe=None, trading_days=None, field=FUTURES_MINUTE_FIELDS, freq='m'):
    """
    Load futures minute data concurrently.
    Available data: closePrice, highPrice, lowPrice, openPrice, turnoverVol, clearingDate, barTime, tradeDate

    Args:
        universe (list of str): futures universe list
        trading_days (list of datetime.datetime): trading days list
        field (list of string): needed fields
    Returns:
        dict of str=>DataFrame: key-->field，value-->DataFrame

    Examples:
        >> universe = ['IF1601']
        >> trading_days = get_trading_days()
        >> equity_data = load_minute_futures_data(universe, trading_days, ['closePrice'])

    """
    universe = list(universe)
    trading_days_index = [dt.strftime("%Y-%m-%d") for dt in trading_days]
    trading_days = [dt.strftime("%Y%m%d") for dt in trading_days]
    trading_days_length = len(trading_days)
    universe_length = len(universe)

    batch_size = max(1, BATCH_MINUTE_DATA_SIZE // trading_days_length)
    batches = [universe[i:min(i+batch_size, universe_length)] for i in range(0, universe_length, batch_size)]

    def _worker(index, batch):
        mkt_data = MktMinuteFuturesDataGet(
            tickers=batch, start=trading_days[0], end=trading_days[-1], freq=freq)
        return index, mkt_data

    with ThreadPoolExecutor(MAX_THREADS) as pool:
        requests = [pool.submit(_worker, idx, bat) for (idx, bat) in enumerate(batches)]
        responses = {future.result()[0]: future.result()[1] for future in as_completed(requests)}

    data_all = {}
    zero_item = []
    zeros = [zero_item for _ in xrange(trading_days_length)]
    for response in sorted(responses.items(), key=lambda x: x[0]):
        _, data = response
        data_all.update(data)
    data_all = {key: pd.DataFrame.from_dict(item).set_index('clearingDate', drop=False) for (key, item) in data_all.iteritems()}
    data_all = dict(pd.Panel.from_dict(data_all).swapaxes(0, 2))
    for var, values in data_all.iteritems():
        for sec in set(universe) - set(data_all[var].keys()):
            values[sec] = zeros
        for ticker, t_minute in values.iteritems():
            values[ticker] = map(np.array, t_minute)
        data_all[var] = pd.DataFrame(values, index=trading_days_index, columns=universe)
    return data_all
