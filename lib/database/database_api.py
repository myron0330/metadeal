# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Database API.
#     Desc: define general database API for the service.
# **********************************************************************************#
import bisect
import DataAPI
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed


BATCH_DAILY_DATA_SIZE = 20000
MARKET_DATA_BATCH_COEFFICIENT = 1
BATCH_MINUTE_DATA_SIZE = 200
BATCH_FILTER_DAY_SIZE = 100
MAX_THREADS = 5
MIN_BATCH_SIZE = 8


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


def load_daily_futures_data(universe, trading_days, attributes='closePrice', **kwargs):
    """
    Load daily futures data.

    Args:
        universe(list): universe symbols list
        trading_days(list): trading days list
        attributes(string or list): available attribute fields
                [u'secID', u'ticker', u'exchangeCD', u'secShortName', u'tradeDate',
                 u'contractObject', u'contractMark', u'preSettlePrice', u'preClosePrice',
                 u'openPrice', u'highestPrice', u'lowestPrice', u'closePrice',
                 u'settlePrice', u'turnoverVol', u'turnoverValue', u'openInt', u'CHG',
                 u'CHG1', u'CHGPct', u'mainCon', u'smainCon']
    Returns:
        dict: key-->attribute, value-->DataFrame
    """
    universe = list(universe)
    trading_days = sorted([trading_day.strftime("%Y%m%d") for trading_day in trading_days])
    trading_day_length = len(trading_days)
    symbols_length = len(universe)
    attributes = attributes.split() if isinstance(attributes, basestring) else list(attributes)

    batch_size = max(MIN_BATCH_SIZE, BATCH_DAILY_DATA_SIZE // trading_day_length)
    batches = [universe[i:min(i+batch_size, symbols_length)] for i in range(0, symbols_length, batch_size)]

    def _loader(index, batch):
        """
        Load daily futures data from DataAPI

        Args:
            index(int): batch index
            batch(list): batch tickers

        Returns:
            int, dict: index and data dict
        """
        attribute_to_database = {
            'highestPrice': 'highPrice',
            'lowestPrice': 'lowPrice',
            'settlePrice': 'settlementPrice',
            'preSettlePrice': 'preSettlementPrice',
        }
        raw_data = DataAPI.MktFutdGet(ticker=batch,
                                      beginDate=trading_days[0],
                                      endDate=trading_days[-1])
        raw_data.rename(columns=attribute_to_database, inplace=True)
        raw_data['symbol'] = raw_data.ticker.apply(lambda x: x.upper())
        result = dict()
        for _ in attributes:
            result[_] = raw_data.pivot(index='tradeDate', columns='symbol', values=_)
        return index, result

    with ThreadPoolExecutor(MAX_THREADS) as pool:
        requests = [pool.submit(_loader, idx, bat) for (idx, bat) in enumerate(batches)]
        responses = {data.result()[0]: data.result()[1] for data in as_completed(requests)}
    data_all = {}
    for attribute in attributes:
        for response in sorted(responses.items(), key=lambda x: x[0]):
            _, data = response
            if attribute not in data_all:
                data_all[attribute] = data[attribute]
            else:
                data_all[attribute] = pd.concat([data_all[attribute], data[attribute]], axis=1)
    return data_all



def load_minute_futures_data(*args, **kwargs):
    """

    Args:
        *args:
        **kwargs:

    Returns:

    """
    return


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
    universe = ['RB1810', 'RM809']
    trading_days = get_trading_days('20180301', '20180401')
    print load_daily_futures_data(universe, trading_days, attributes=['closePrice', 'turnoverValue'])
