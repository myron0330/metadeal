# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Database API.
#     Desc: define general database API for the service.
# **********************************************************************************#
import os
import json
import bisect
import DataAPI
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed


os.environ['privilege'] = json.dumps({'basic': 1})
BATCH_DAILY_DATA_SIZE = 20000
MARKET_DATA_BATCH_COEFFICIENT = 1
BATCH_MINUTE_DATA_SIZE = 200
BATCH_FILTER_DAY_SIZE = 100
MAX_THREADS = 5
MIN_BATCH_SIZE = 8
FUTURES_DAILY_FIELDS = ['tradeDate', 'openPrice', 'highPrice', 'lowPrice', 'closePrice', 'settlementPrice',
                        'volume', 'openInterest', 'preSettlementPrice', 'turnoverVol', 'turnoverValue']
FUTURES_MINUTE_FIELDS = ['tradeDate', 'clearingDate', 'barTime', 'openPrice', 'highPrice', 'lowPrice',
                         'closePrice', 'volume', 'tradeTime', 'turnoverVol', 'turnoverValue', 'openInterest']


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


def load_daily_futures_data(universe, trading_days, attributes=FUTURES_DAILY_FIELDS, **kwargs):
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
        raw_data['volume'] = raw_data['turnoverVol']
        raw_data['openInterest'] = raw_data['openInt']
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
        import os
        import json
        data_cube_fields = [
            'openPrice', 'highPrice', 'lowPrice',
            'closePrice', 'turnoverVol', 'turnoverValue',
            'openInterest', 'tradeDate'
        ]
        os.environ['privilege'] = json.dumps({'basic': 1})
        data = DataAPI.get_data_cube(symbol=batch, field=data_cube_fields,
                                     start=trading_days[0],
                                     end=trading_days[-1],
                                     freq='m')
        result = dict()

        for symbol in batch:
            symbol_data = data[symbol]
            symbol_data.dropna(inplace=True)
            trade_dates = symbol_data['tradeDate'].tolist()
            sorted_trade_dates = sorted(set(trade_dates), key=trade_dates.index)
            next_date_mapping = dict(zip(sorted_trade_dates[:-1], sorted_trade_dates[1:]))

            def _transfer_clearing_date(trade_time):
                """
                Transfer clearing date based on  trade time.
                """
                date, minute = trade_time.split(' ')
                if minute >= '21:00':
                    return next_date_mapping[date]
                return date

            symbol_data['tradeTime'] = symbol_data.index
            symbol_data['clearingDate'] = symbol_data.tradeTime.apply(_transfer_clearing_date)
            symbol_data['barTime'] = map(lambda x: x.split(' ')[-1], symbol_data.index)
            symbol_data['volume'] = symbol_data['turnoverVol']
            symbol_data['symbol'] = symbol
            frame_list = [symbol_data[symbol_data.clearingDate == _] for _ in set(symbol_data['clearingDate'])]
            symbol_result = dict()
            keys = symbol_data.keys()
            for index, key in enumerate(keys):
                symbol_result[key] = np.array([_[key].tolist() for _ in frame_list]).tolist()
                if key in ['clearingDate', 'symbol']:
                    result_length = len(symbol_result[key])
                    valid_array = reduce(lambda x, y: x + y, symbol_result[key])
                    adjusted_array = sorted(set(valid_array), key=valid_array.index)
                    symbol_result[key] = adjusted_array * int(result_length / len(adjusted_array))
            result[symbol] = symbol_result
        return index, result

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
    # print get_trading_days(datetime(2015, 1, 1), datetime(2015, 2, 1))
    # print get_direct_trading_day(datetime(2015, 1, 1), step=0, forward=True)
    # print get_direct_trading_day(datetime(2015, 1, 1), step=1, forward=True)
    # print get_direct_trading_day(datetime(2015, 1, 1), step=1, forward=False)
    # print get_futures_base_info(['RB1810'])
    # print get_futures_main_contract(contract_objects=['RB', 'AG'], start='20180401', end='20180502')
    # print load_daily_futures_data(['RB1810', 'RM809'],
    #                               get_trading_days('20180301', '20180401'),
    #                               attributes=['closePrice', 'turnoverValue'])
    # print load_daily_futures_data(['RB1810', 'RM809'],
    #                               get_trading_days('20180301', '20180401'))
    print load_minute_futures_data(['RB1810', 'RM809'], get_trading_days('20180614', '20180616'))
