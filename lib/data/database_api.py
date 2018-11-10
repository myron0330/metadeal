# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Database API.
#     Desc: define general database API for the service.
# **********************************************************************************#
import os
import json
import bisect
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from api_client import Client
from lib.const import (
    BASE_FUTURES_PATTERN,
    CONTINUOUS_FUTURES_PATTERN,
    MULTI_FREQ_PATTERN
)
from mongodb_api import (
    query_from_mongodb,
    dump_schema_to_mongodb
)
from redis_api import (
    query_from_redis,
    dump_schema_to_redis,
    delete_keys_redis,
    delete_items_in_redis
)


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
client = Client()
client.init('602bada78f4eb803470a5b8754eb956da631fc072e116deb39b7c85b94d070dc')
# client.init('12add2cfd90efc05ad9bb470362da2f6559f4c3b38839be9a72668bef4c7aad8')


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


def get_end_date():
    """
    End date.
    """
    return normalize_date(datetime.today())


def get_trading_days(start, end):
    """
    Get trading days.
    Args:
        start(string or datetime): start date
        end(string or datetime): end date

    Returns:
        list of datetime: trading days list

    """
    start = normalize_date(start).strftime('%Y%m%d')
    end = normalize_date(end).strftime('%Y%m%d')
    url = '/api/master/getTradeCal.json?field=&exchangeCD=XSHG&beginDate={}&endDate={}'.format(start, end)
    code, data = client.getData(url)
    if code != 200:
        raise Exception
    data = json.loads(data)['data']
    return map(lambda x: normalize_date(x['calendarDate']), filter(lambda x: x['isOpen'] == 1, data))


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
    date = normalize_date(date)
    start_date = date - timedelta(100)
    end_date = date + timedelta(100)
    target_trading_days = get_trading_days(start=start_date,
                                           end=end_date)
    date_index = bisect.bisect_left(target_trading_days, date)
    target_index = date_index + (1 if forward else -1) * step
    return target_trading_days[target_index]


def get_data_cube(symbols, field, start, end=None, freq='1d'):
    """
    Get data cube based on DataAPI.

    Args:
        symbols(list or string): ordinary and artificial future contracts.
        field(list or string): attribute fields.
        start(datetime.datetime or string): start date
        end(datetime.datetime or string): end date
        freq(string): frequency
        style(string): data style, ast/sat/tas.
                        a: attribute, s: symbol, t: time
        adj(string): 'pre' represent pre_adj
        **kwargs: kwargs arguments

    Returns:
        pandas.Panel: pandas panel.
    """
    market_daily_fields_set = set(FUTURES_DAILY_FIELDS)
    market_minute_fields_set = set(FUTURES_MINUTE_FIELDS)

    def _format_array_of_string(param, param_name):
        if isinstance(param, basestring):
            if ',' in param:
                param = param.split(',')
            elif ';' in param:
                param = param.split(';')
            else:
                param = [param]
        elif isinstance(param, list):
            for item in param:
                if not isinstance(item, basestring):
                    print ("Parameters %s only support string or list of string inputs." % param_name)
                    return list()
        return [item.strip() for item in param if isinstance(item, basestring)]

    symbols = _format_array_of_string(symbols, "symbol")
    if not symbols:
        return

    field = _format_array_of_string(field, "field")
    if not field:
        return

    if isinstance(freq, basestring):
        if freq not in ['1d', '1m', 'd', 'm', '5m', '15m', '30m', '60m']:
            print ("Only support '1d','1m','5m', '15m', '30m', '60m'")
            return
    else:
        print ("Param 'freq' only support string inputs.")

    if adj is not None and adj != 'pre':
        print (u"Only support 'pre' input of 'adj' parameter for now")

    start_date = normalize_date(start)
    if start_date is None:
        return

    base_futures = [x for x in symbols if BASE_FUTURES_PATTERN.match(x)]
    continuous_futures = [x for x in symbols if CONTINUOUS_FUTURES_PATTERN.match(x)]

    futures = list(set(base_futures) | set(continuous_futures))

    if freq in ['1m', '5m', '15m', '30m', '60m', 'm']:
        start_date = max(start_date, datetime(2010, 1, 1))

    mkt_daily_fields = set(field) & market_daily_fields_set
    mkt_minute_fields = (set(field) & market_minute_fields_set)
    mkt_fields = mkt_daily_fields | mkt_minute_fields

    fs_fields = list(set(field) & set())
    rest = set(field) - set(mkt_fields) - set(fs_fields)
    if rest:
        print ("Invalid fields: %s" % list(rest))

    end_date = normalize_date(end) if end is not None else get_end_date()
    if end_date is None:
        return
    if end_date < start_date:
        print ("start date is earlier than end date.")
        return
    trading_days = get_trading_days(start_date, end_date)
    if len(trading_days) == 0:
        print ("Date period among start:%s, end:%s contains no trading days." % (start, end))
        return
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    filtered_futures = list()
    if futures:
        futures_info_frame = load_futures_base_info(symbols)
        futures_info_frame.index = futures_info_frame.symbol
        futures_info_dict = futures_info_frame[['listDate', 'lastTradeDate']].T.to_dict()
        for future in futures:
            if future in futures_info_dict:
                list_date, last_trade_date = \
                    futures_info_dict[future]['listDate'], futures_info_dict[future]['lastTradeDate']
                if start_date_str < last_trade_date and end_date_str > list_date:
                    filtered_futures.append(future)
            else:
                filtered_futures.append(future)
        if len(set(futures) - set(filtered_futures)) > 0:
            print ("%s are unreachable in the response because of the list periods." %
                   list(set(futures) - set(filtered_futures)))

    invalid_symbol = list(set(symbols) - set(futures))
    if invalid_symbol:
        print ("can not recognize symbol parameters %s， please check." % invalid_symbol)

    symbols = list(set(filtered_futures))
    if len(symbols) == 0:
        return pd.Panel()
    raise NotImplementedError


def load_futures_daily_data(universe, trading_days, attributes=FUTURES_DAILY_FIELDS, **kwargs):
    """
    Load futures daily data.

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
    universe = filter(lambda x: not CONTINUOUS_FUTURES_PATTERN.match(x), universe)
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
        ticker = ','.join(batch)
        begin_date, end_date = trading_days[0], trading_days[-1]
        url = '/api/market/getMktFutd.json?ticker={}&beginDate={}&endDate={}'.format(ticker, begin_date, end_date)
        code, data = client.getData(url)
        if code != 200:
            raise Exception
        raw_data = pd.DataFrame(json.loads(data)['data'])
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


def load_futures_minute_data(universe=None, trading_days=None, field=FUTURES_MINUTE_FIELDS, freq='m'):
    """
    Load futures minute data concurrently.
    Available data: closePrice, highPrice, lowPrice, openPrice, turnoverVol, clearingDate, barTime, tradeDate

    Args:
        universe(list of str): futures universe list
        trading_days(list of datetime.datetime): trading days list
        field(list of string): needed fields
        freq(string): frequency string

    Returns:
        dict of str=>DataFrame: key-->field，value-->DataFrame

    Examples:
        >> universe = ['IF1601']
        >> trading_days = get_trading_days()
        >> equity_data = load_minute_futures_data(universe, trading_days, ['closePrice'])

    """
    universe = filter(lambda x: not CONTINUOUS_FUTURES_PATTERN.match(x), universe)
    trading_days_index = [dt.strftime("%Y-%m-%d") for dt in trading_days]
    trading_days = [dt.strftime("%Y%m%d") for dt in trading_days]
    trading_days_length = len(trading_days)
    universe_length = len(universe)
    unit = int(freq[:-1]) if MULTI_FREQ_PATTERN.match(freq) else 1
    batch_size = 1
    batches = [universe[i:min(i+batch_size, universe_length)] for i in range(0, universe_length, batch_size)]

    def _loader(index, batch):
        import os
        import json

        if not batch:
            return index, dict()
        os.environ['privilege'] = json.dumps({'basic': 1})
        url = '/api/market/getFutureBarHistDateRange.json?' \
              'instrumentID={}&startDate={}&endDate={}&unit={}'.format(batch[0],
                                                                       trading_days[0],
                                                                       trading_days[-1],
                                                                       unit)
        code, resp_data = client.getData(url)
        if code != 200:
            raise Exception
        frame = pd.DataFrame(json.loads(resp_data)['data'][0]['barBodys'])
        frame.rename(columns={
            'dataDate': 'tradeDate',
            'totalVolume': 'turnoverVol',
            'totalValue': 'turnoverValue',
            'clearingDay': 'clearingDate'},
            inplace=True)
        frame['tradeTime'] = frame.tradeDate + ' ' + frame.barTime
        frame.index = frame.tradeTime
        raw_data = pd.Panel({batch[0]: frame}).replace([None], np.nan)
        result = dict()

        for symbol in batch:
            symbol_data = raw_data[symbol]
            symbol_data.dropna(inplace=True)
            symbol_data['volume'] = symbol_data['turnoverVol']
            symbol_data['symbol'] = symbol
            frame_list = [symbol_data[symbol_data.clearingDate == _] for _ in set(symbol_data['clearingDate'])]
            symbol_result = dict()
            keys = symbol_data.keys()
            for key in keys:
                symbol_result[key] = np.array([_[key].tolist() for _ in frame_list]).tolist()
                if key in ['clearingDate', 'symbol']:
                    result_length = len(symbol_result[key])
                    valid_array = reduce(lambda x, y: x + y, symbol_result[key])
                    adjusted_array = sorted(set(valid_array), key=valid_array.index)
                    symbol_result[key] = adjusted_array * int(result_length / len(adjusted_array))
            result[symbol] = symbol_result
        return index, result

    with ThreadPoolExecutor(MAX_THREADS) as pool:
        requests = [pool.submit(_loader, idx, bat) for (idx, bat) in enumerate(batches)]
        responses = {future.result()[0]: future.result()[1] for future in as_completed(requests)}

    data_all = {}
    zero_item = []
    zeros = [zero_item for _ in xrange(trading_days_length)]
    for response in sorted(responses.items(), key=lambda x: x[0]):
        _, data = response
        data_all.update(data)
    data_all = {key: pd.DataFrame.from_dict(item).set_index('clearingDate', drop=False)[field]
                for (key, item) in data_all.iteritems()}
    data_all = dict(pd.Panel.from_dict(data_all).swapaxes(0, 2))
    for var, values in data_all.iteritems():
        for sec in set(universe) - set(data_all[var].keys()):
            values[sec] = zeros
        for ticker, t_minute in values.iteritems():
            values[ticker] = map(np.array, t_minute)
        data_all[var] = pd.DataFrame(values, index=trading_days_index, columns=universe)
    return data_all


def load_futures_rt_minute_data(universe):
    """
    Load futures real-time minute data of current date.

    Args:
        universe(list): futures list

    Returns:
        dict: minute bar data
    """
    assert isinstance(universe, (list, tuple, set))
    universe_string = ','.join(universe)
    url = '/api/market/getFutureBarRTIntraDay.json?instrumentID={}&unit=1'.format(universe_string)
    code, data = client.getData(url)
    if code != 200:
        raise Exception
    data = json.loads(data)['data']

    def _transfer_bar(response):
        bar_data = dict()
        for item in response:
            symbol = item['ticker'].upper()
            bar_data[symbol] = item['barBodys']
        return bar_data

    return _transfer_bar(data)


def load_futures_base_info(symbols=None):
    """
    Get futures base info.

    Args:
        symbols(list): basic future symbols
    """
    url = '/api/future/getFutu.json'
    if symbols:
        url = '?'.join([url, 'ticker={}'.format(','.join(symbols))])
        code, data = client.getData(url)
    else:
        code, data = client.getData(url)
    if code != 200:
        raise Exception
    data = pd.DataFrame(json.loads(data)['data'])
    rename_dict = {
        'ticker': 'symbol'
    }
    data.rename(columns=rename_dict, inplace=True)
    data.symbol = data.symbol.apply(lambda x: x.upper())
    return data


def load_futures_main_contract(contract_objects=None, trading_days=None, start=None, end=None):
    """
    Get futures main contract

    Args:
        contract_objects(list): continuous symbols
        trading_days(list): trading days
        start(string or datetime.datetime): start date
        end(string or datetime.datetime): end date
    """
    start = normalize_date(start or trading_days[0]).strftime('%Y%m%d')
    end = normalize_date(end or trading_days[-1]).strftime('%Y%m%d')
    url = '/api/market/getMktMFutd.json?mainCon=1&startDate={}&endDate={}'.format(start, end)
    code, data = client.getData(url)
    if code != 200:
        raise Exception
    data = pd.DataFrame(json.loads(data)['data'])
    data.ticker = data.ticker.apply(lambda x: x.upper())
    frame = data.pivot(index='tradeDate', columns='contractObject', values='ticker')
    if contract_objects:
        return frame[contract_objects]
    return frame


def query_from_(database, schema_type, portfolio_id=None, date=None, **kwargs):
    """
    Query schema from database

    Args:
        database(string): database name | {'mongodb', 'redis', 'all'}
        schema_type(string): schema type
        portfolio_id(string or list or dict): optional, portfolio id or portfolio ids
        date(string): optional, query date
    """
    if database == 'mongodb':
        return query_from_mongodb(schema_type, portfolio_id=portfolio_id, date=date, **kwargs)
    if database == 'redis':
        return query_from_redis(schema_type, portfolio_id=portfolio_id, **kwargs)


def dump_to_(database, schema_type, schema, unit_dump=True, **kwargs):
    """
    Dump schema to database

    Args:
        database(string): database name | {'mongodb', 'redis', 'all'}
        schema_type(string): schema type
        schema(schema or dict of schema): schema object
        unit_dump(boolean): whether to do unit dump
    """
    if database in ['mongodb', 'all']:
        dump_schema_to_mongodb(schema_type, schema, unit_dump=unit_dump)
    if database in ['redis', 'all']:
        dump_schema_to_redis(schema_type, schema, **kwargs)


def delete_(database, *collections):
    """
    Delete collections in database

    Args:
        database(string): database name | {'mongodb', 'redis', 'all'}
    """
    if database in ['redis']:
        return delete_keys_redis(*collections)


def delete_items_(database, schema_type, items=None, **kwargs):
    """
    Delete items in a database table or hash map

    Args:
        database(string): database name | {'mongodb', 'redis', 'all'}
        schema_type(string): schema type
        items(string or list or dict): optional, portfolio id or portfolio ids
    """
    if database in ['redis', 'all']:
        if isinstance(items, (str, unicode)):
            keys = [items]
        elif isinstance(items, (list, tuple, set, dict)):
            keys = list(items)
        else:
            keys = None
        if keys:
            delete_items_in_redis(schema_type, keys=keys)
    if database in ['mongodb', 'all']:
        raise NotImplementedError


__all__ = [
    'delete_',
    'delete_items_',
    'dump_to_',
    'get_data_cube',
    'get_direct_trading_day',
    'get_end_date',
    'get_trading_days',
    'load_futures_rt_minute_data',
    'load_futures_base_info',
    'load_futures_daily_data',
    'load_futures_main_contract',
    'load_futures_minute_data',
    'normalize_date',
    'query_from_'
]
