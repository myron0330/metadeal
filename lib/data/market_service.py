# -*- coding: utf-8 -*-
import re
import bisect
import datetime
import numpy as np
import pandas as pd
from collections import OrderedDict, defaultdict
from copy import copy
from utils.adjust_utils import *
from utils.datetime_utils import (
    get_end_date,
    get_previous_trading_date,
    get_next_trading_date
)
from . asset_service import AssetType, AssetService
from .. data.universe_service import UniverseService, Universe
from .. core.enums import SecuritiesType
from .. database.database_api import (
    load_futures_daily_data,
    load_futures_minute_data,
)

from .. const import (
    CONTINUOUS_FUTURES_PATTERN,
    FUTURES_DAILY_FIELDS,
    FUTURES_MINUTE_FIELDS,
    TRADE_ESSENTIAL_MINUTE_BAR_FIELDS,
    REAL_TIME_MINUTE_BAR_FIELDS
)

MULTI_FREQ_PATTERN = re.compile('(\d+)m')
CURRENT_BAR_FIELDS_MAP = {
    'barTime': 'barTime',
    'closePrice': 'closePrice',
    'highPrice': 'highPrice',
    'lowPrice': 'lowPrice',
    'openPrice': 'openPrice',
    'totalValue': 'turnoverValue',
    'totalVolume': 'turnoverVol'
}


def _ast_stylish(raw_data_dict, symbols, time_bars, fields, style, rtype='frame'):
    """
    将raw_data_dict按照style和rtype转化成对应的格式，其中raw_data_dict为满足ast格式的dict

    Args:
        raw_data_dict(dict of ndarray, 'ast' style):  需要转化的原始数据，一般为MarketData.slice返回数据，样式为ast
        symbols(list of str): raw_data_dict中包含的symbol列表，需要与ndarray中colume对应
        time_bars(list of str): raw_data_dict中包含的time_bars列表，需要与ndarray中的index对应
        fields(list of str): raw_data_dict中包含的attributes列表，与其key值对应
        style('tas'/'ast'/'sat'): 需要转化为的目标样式
        rtype('frame'/'array'): 需要转化为的目标格式，dict或者DataFrame

    Returns:
        dict or frame：将raw_data_dict转化完成后，满足style样式的rtype类型的数据结构
    """
    if style == 'ast':
        history_data = {}
        for attribute in fields:
            if rtype == 'frame':
                history_data[attribute] = pd.DataFrame(data=raw_data_dict[attribute], columns=symbols, index=time_bars)
            if rtype == 'array':
                history_data[attribute] = {s: raw_data_dict[attribute][:, i] for (i, s) in enumerate(symbols)}
                history_data['time'] = {s: np.array(time_bars) for s in symbols}
    elif style == 'sat':
        history_data = {}
        for idx, symbol in enumerate(symbols):
            if rtype == 'frame':
                history_data[symbol] = pd.DataFrame(data={a: raw_data_dict[a][:, idx] for a in fields}, index=time_bars, columns=fields)
            if rtype == 'array':
                history_data[symbol] = {a: raw_data_dict[a][:, idx] for a in fields}
                history_data[symbol]['time'] = np.array(time_bars)
    elif style == 'tas':
        history_data = {}
        for idx, trade_date in enumerate(time_bars):
            if rtype == 'frame':
                history_data[trade_date] = pd.DataFrame(data={a: raw_data_dict[a][idx, :] for a in fields},
                                                        index=symbols, columns=fields)
            if rtype == 'array':
                history_data[trade_date] = {a: raw_data_dict[a][idx, :] for a in fields}
                history_data[trade_date]['symbol'] = np.array(symbols)
    else:
        # raise ValueError('Exception in "MarketService._ast_stylish": '
        #                  'history style \'%s\' is not supported here, please refer to document for details' % style)
        raise Exception()
    return history_data


def _ast_slice(raw_data_dict, symbols, end_time_str, fields, start_time_str=None, check_attribute='closePrice',
               time_range=None, cached_trading_days=None, **options):
    """
    对raw_data_dict进行slice操作，其中raw_data_dict为满足ast结构的dict，其中time_range和start_time_str必须有一个有值

    Args:
        raw_data_dict(dict of DataFrame, ast style): 需要进行slice的原始数据
        symbols(list of str): 需要slice出的符号数据
        end_time_str(date formatted str): slice出数据的截止时间，日线数据格式为'YYYYmmdd'，分钟线数据格式为'YYYYmmdd HH:MM'
        fields(list of str): 需要slice出的字段列表
        start_time_str(date formatted str or None): slice出数据的开始时间，日线数据格式为'YYYYmmdd'，分钟线数据格式为'YYYmmdd HH:MM'，该字段和time_range必须有一个不为空
        check_attribute(str): 用于检查raw_data_dict是否为空的字段，raw_data_dict不含有该字段，则表示数据为空
        time_range(int or None): 从end_time_str向前slice时间长度
    """
    time_index = raw_data_dict[check_attribute].index
    end_time_str = end_time_str if end_time_str in time_index \
        else cached_trading_days[min(bisect.bisect_right(cached_trading_days, end_time_str), len(cached_trading_days) - 1)]
    end_time_str = min(end_time_str, time_index[-1])
    end_pos = time_index.get_loc(end_time_str) + 1 if end_time_str in time_index else 0
    if time_range is None:
        start_pos = time_index.get_loc(start_time_str) if start_time_str in time_index \
            else bisect.bisect_left(time_index, start_time_str)
    else:
        start_pos = max(end_pos - time_range, 0)
    time_bars = time_index.tolist()[start_pos:end_pos]
    result = {}
    for attribute in fields:
        df_matrix = raw_data_dict[attribute].as_matrix()
        symbol_idxs = [raw_data_dict[attribute].columns.get_loc(s) for s in symbols]
        result[attribute] = df_matrix[start_pos: end_pos, symbol_idxs]
    return result, time_bars


def _rolling_load_data(data, trading_days, universe, max_cache_days, data_load_func, fields):
    """
    加载trading_days对应的行情数据，其中data中已有数据不做从重新加载

    Args:
        data(dict of dict): 原有数据，其中含有的trading_days数据不会做重新加载
        trading_days(list of datetime): 需要滚动加载数据的交易日列表
        universe(list of str): 需要滚动加载的股票池，必须与data中的universe保持一致
        max_cache_days(int or None): 需要保留的data中原有数据的长度，如果为None则表示保留所有原有数据
        data_load_func(func: universe, trading_days, fields => dict of dict): 数据加载函数
        fields(list of str): 需要加载的数据字段

    Returns:
        dict of DataFrame, ast style: 滚动加载之后新的数据内容
    """
    data = copy(data)
    if len(data) == 0:
        trading_days_in_loaded = []
    else:
        trading_days_in_loaded = [datetime.datetime.strptime(t, '%Y-%m-%d') for t in data.values()[0].index]
    target_days = sorted(set(trading_days_in_loaded) | set(trading_days))
    if max_cache_days is not None:
        target_days = target_days[-max_cache_days:]
    to_reserve_days = sorted(set(trading_days_in_loaded) & set(target_days))
    to_load_trading_days = sorted(set(target_days) - set(to_reserve_days))
    if len(to_load_trading_days) == 0:
        return data
    new_data = data_load_func(universe, to_load_trading_days, fields)
    if len(data) == 0 or len(to_reserve_days) == 0:
        for var in fields:
            if var in new_data:
                data[var] = new_data[var].sort_index()
    else:
        to_reserve_tdays = [t.strftime('%Y-%m-%d') for t in to_reserve_days]
        for var in fields:
            if var in new_data and isinstance(new_data[var], pd.DataFrame):
                data[var] = pd.concat([data[var].loc[to_reserve_tdays], new_data[var]], axis=0).sort_index()
            else:
                data[var] = data[var].loc[to_reserve_tdays].sort_index()
    return data


def _uncompress_minute_bars(minute_bars, columns, index_field, index_series=None):
    """
    展开压缩的分钟线数据，要求该分钟线数据必须在时间上可对齐
    """
    result_dict = {}
    for column in columns:
        result_dict[column] = np.concatenate(minute_bars[column].as_matrix())
    result_df = pd.DataFrame(result_dict)
    if index_series is not None:
        result_df[index_field] = pd.Series(index_series, name=index_field)
    return result_df.set_index(index_field)


def _concat_data(data_list, rtype='frame', axis=0):
    """
    对dict或dataframe数据进行拼装的共用方法

    Args:
        data_list(list of dict of dict): 需要进行拼装的数据
        rtype(dict or frame): 原始数据类型
        axis(0 or 1): 0表示row拼装，1表示column拼装

    Returns:
        dict of dict or dict of DataFrame
    """
    data_list = [d for d in data_list if d is not None]
    if rtype == 'frame':
        return pd.concat(data_list, axis=axis)
    elif rtype == 'array':
        result = {}
        if axis == 1:
            for data in data_list:
                result.update(data)
        if axis == 0:
            for data in data_list:
                for key, value in data.iteritems():
                    result.setdefault(key, [])
                    result[key].append(value)
            for key, value in result.iteritems():
                # if key != 'symbol':
                #     result[key] = np.concatenate(value)
                result[key] = np.concatenate(value)
        return result


def _append_data(raw_data, sliced_data, style, rtype='frame'):
    """
    将slice并stylish之后的数据进行组合拼装

    Args:
        raw_data(dict of DataFrame or dict of dict): 需要进行拼装数据的原始数据，格式和style及rtype对应
        sliced_data(dict of DataFrame or dict of dict): 需要进行拼装数据的新数据，格式和raw_data必须保持一致
        style(ast or sat or tas): 拼装数据的数据格式
        rtype(frame or dict): 拼装数据的数据类型

    Returns:
        dict of dict or dict of DataFrame
    """
    result = {}
    if style == 'ast':
        for attribute in set(raw_data.keys()) | set(sliced_data.keys()):
            a_data = _concat_data([raw_data.get(attribute, None), sliced_data.get(attribute, None)], axis=1, rtype=rtype)
            result[attribute] = a_data
    if style == 'sat':
        result.update(raw_data)
        result.update(sliced_data)
    if style == 'tas':
        for tdays in set(raw_data.keys()) | set(sliced_data.keys()):
            t_data = _concat_data([raw_data.get(tdays), sliced_data.get(tdays)], axis=0, rtype=rtype)
            result[tdays] = t_data
    return result


def _load_current_bars_by_now(current_trading_day, universe, fields, load_func):
    """
    加载交易日当天日内已产生的分钟线压缩成AST array
    Args:
        current_trading_day(datetime.datetime): 模拟交易当前交易日
        universe(list or set): equity symbol list or set
        fields(list of string): attribute list
        load_func(func): 加载当前交易日已聚合的接口方法

    Returns(dict):
        attribute: df

    """
    sta_data = load_func(current_trading_day, universe, fields)

    a_df_dict = {}
    current_str = current_trading_day.strftime('%Y-%m-%d')
    # todo: 期货未加载持仓量的分钟线bar数据
    for field in REAL_TIME_MINUTE_BAR_FIELDS:
        s_array_dict = {}
        default_array = np.array([], dtype='S') if field in ['barTime', 'tradeTime'] else np.array([])
        for symbol in universe:
            s_array_dict.update(
                {symbol: np.array([e[field] for e in sta_data[symbol]] if symbol in sta_data else default_array)})
        a_df_dict.update({
            CURRENT_BAR_FIELDS_MAP[field]: pd.DataFrame.from_dict({current_str: s_array_dict}, orient='index')})
    a_df_dict['barTime'] = a_df_dict['barTime'].applymap(lambda x: np.char.encode(x))

    def _map_to_date(bar_time, prev_trading_day, prev_next_date, curr_trading_day):
        """
        返回bar_time所对应的日期
        """
        if bar_time.startswith('2'):
            date = prev_trading_day
        elif bar_time[:2] < '09':
            date = prev_next_date
        else:
            date = curr_trading_day
        return date + bar_time

    vmap_date = np.vectorize(_map_to_date, otypes=[basestring])
    if 'tradeTime' in fields or 'tradeDate' in fields:
        prev_trading_day = get_previous_trading_date(current_trading_day)
        prev_trading_str = prev_trading_day.strftime('%Y-%m-%d ')
        prev_next_day = prev_trading_day + datetime.timedelta(days=1)
        prev_next_str = prev_next_day.strftime('%Y-%m-%d ')
        curr_trading_str = current_trading_day.strftime('%Y-%m-%d ')
        trade_time_df = a_df_dict['barTime'].applymap(lambda x:
                                                      vmap_date(x, prev_trading_str, prev_next_str, curr_trading_str))
        if 'tradeTime' in fields:
            a_df_dict['tradeTime'] = trade_time_df
        if 'tradeDate' in fields:
            a_df_dict['tradeDate'] = trade_time_df.applymap(lambda x:
                                                            np.core.defchararray.rjust(x.astype(str), width=10))
    if 'clearingDate' in fields:
        a_df_dict['clearingDate'] = a_df_dict['barTime'].applymap(lambda x:
                                                                  np.full_like(x, fill_value=current_str))
    return a_df_dict


class MarketService(object):
    """
    行情数据服务类
    * asset_service: AssetService
    * market_data_list: 含各AssetType的MarketData的集合
    * minute_bar_map: 含分钟线行情时的每个交易日bar_time
    * universe_service: UniverseService
    """

    def __init__(self):
        self.stock_market_data = None
        self.futures_market_data = None
        self.fund_market_data = None
        self.otc_fund_market_data = None
        self.index_market_data = None
        self.market_data_list = list()
        self.asset_service = None
        self.universe_service = None
        self.minute_bar_map = dict()
        self.calendar_service = None
        self.daily_bars_loaded_days = None
        self.minute_bars_loaded_days = None
        self._available_daily_fields = None
        self._available_minute_fields = None

    def batch_load_data(self, universe=None,
                        calendar_service=None,
                        universe_service=None,
                        asset_service=None,
                        **kwargs):
        """
        Batch load market data.
        Args:
            universe(list of universe): universe list
            calendar_service(obj): calendar service
            universe_service(obj): universe service
            asset_service(obj): asset service
            **kwargs: key-value parameters

        Returns:
            MarketService(obj): market service
        """
        self.create_with(universe,
                         market_service=self,
                         universe_service=universe_service,
                         asset_service=asset_service,
                         calendar_service=calendar_service)
        self.rolling_load_daily_data(calendar_service.all_trading_days)
        return self

    def subset(self, *args, **kwargs):
        """
        Subset the market service
        """
        return self

    @classmethod
    def create_with_service(cls,
                            asset_service=None,
                            universe_service=None,
                            calendar_service=None,
                            market_service=None):
        """
        通过静态方法创建MarketService实例，market_data_list中含asset_service中各类资产的MarketData

        Args:
            asset_service: AssetService
            universe_service: UniverseService
            calendar_service: CalendarService
            market_service: MarketService

        Returns:
            MarketService
        """
        market_service = market_service or cls()
        market_service.asset_service = asset_service
        market_service.universe_service = universe_service
        market_service.calendar_service = calendar_service
        futures_universe = asset_service.filter_symbols(asset_type=AssetType.FUTURES,
                                                        symbols=universe_service.full_universe)
        if len(futures_universe) > 0:
            market_service.futures_market_data = FuturesMarketData(futures_universe)
            market_service.market_data_list.append(market_service.futures_market_data)
        return market_service

    @classmethod
    def create_with(cls,
                    universe='A',
                    market_service=None,
                    asset_service=None,
                    universe_service=None,
                    calendar_service=None):
        """
        使用universe创建MarketService

        Args:
            universe(list of str or str): MarketService中需要包含的股票池
            market_service(obj): market service
            asset_service(obj): asset service
            universe_service(obj): universe service
            calendar_service(obj): calendar service

        Returns:
            MarketService(obj): market service
        """
        prev_trading_day = get_previous_trading_date(get_end_date().today())
        if universe_service is None:
            if isinstance(universe, Universe):
                pass
            elif isinstance(universe, list):
                universe = Universe(*universe)
            else:
                universe = Universe(universe)
            universe_service = UniverseService(universe, [prev_trading_day])
        asset_service = asset_service or AssetService.from_symbols(universe_service.full_universe)
        return cls.create_with_service(asset_service=asset_service,
                                       universe_service=universe_service,
                                       market_service=market_service,
                                       calendar_service=calendar_service)

    def slice(self, symbols, fields, end_date, freq='d', time_range=1, style='ast', rtype='frame',
              f_adj=None, prepare_dates=None, **options):
        """
        依次对market_data_list各项进行slice

        Args:
            symbols(list of symbol): 对universe中特定symbol的列表进行slice
            fields(list of str): 返回***_bars行情中所选择字段
            end_date: slice截止日期
            freq: 'd' or 'm'
            time_range(int): end_date往前交易日天数
            style: 'ast', 'sat' or 'tas', 默认'ast'
            rtype: 默认'frame'(dict of DataFrame) or 'array'(dict of array)
            f_adj(string): 期货复权类型
            prepare_dates(list of datetime): 为了完成slice，需要确保分钟线已经加载并展开的日期
        Returns:
            dict of dict: 格式视style与rtype参数输入
        """
        result = {}
        if symbols == 'all':
            symbols = list(self.universe_service.full_universe)
        symbols = symbols if isinstance(symbols, list) else [symbols]

        for market_data in self.market_data_list:
            selected_universe = self.asset_service.filter_symbols(asset_type=market_data.asset_type, symbols=symbols)
            if len(selected_universe) != 0:
                result = _append_data(result,
                                      market_data.slice(selected_universe, fields, end_date, freq=freq,
                                                        style=style, time_range=time_range, rtype=rtype,
                                                        f_adj=f_adj, prepare_dates=prepare_dates,
                                                        cached_trading_days=self.calendar_service.cache_all_trading_days,
                                                        **options),
                                      style, rtype=rtype)
        return result

    def batch_load_daily_data(self, trading_days):
        """
        批量加载日线数据

        Args:
            trading_days(list of datetime): 批量加载日线的具体交易日列表
        """
        self.rolling_load_daily_data(trading_days)

    def rolling_load_daily_data(self, trading_days, max_cache_days=None):
        """
        依次对market_data_list中各项MarketData进行日行情加载。

        Args:
            trading_days(list of datetime): backtest时所传入含max_window_history
            max_cache_days(int): market_data_list中daily_bars最大载入天数
        """
        for market_data in self.market_data_list:
            if market_data is not None:
                market_data.rolling_load_daily_data(trading_days, max_cache_days, self.asset_service)
                self.daily_bars_loaded_days = market_data.daily_bars_loaded_days or self.daily_bars_loaded_days

    def rolling_load_minute_data(self, trading_days, max_cache_days=5):
        """
        依次对market_data_list中各项MarketData进行分钟线行情加载。
        Args:
            trading_days(list of datetime.datetime): 批量加载日线的具体交易日列表
            max_cache_days(int): 最大保留的分钟线天数
        """
        for market_data in self.market_data_list:
            if market_data is not None:
                market_data.rolling_load_minute_data(trading_days, max_cache_days)
                self.minute_bars_loaded_days = market_data.minute_bars_loaded_days or self.minute_bars_loaded_days

    def available_fields(self, freq='d'):
        """
        返回日行情或分钟行情可获取attribute
        Args:
            freq('d' or 'm')， 默认'd'

        Returns:
            list of str
        """
        if freq == 'd':
            if self._available_daily_fields is None:
                self._available_daily_fields = set()
                for market_data in self.market_data_list:
                    self._available_daily_fields |= set(market_data.daily_fields)
            return self._available_daily_fields
        elif freq == 'm':
            if self._available_minute_fields is None:
                self._available_minute_fields = set()
                for market_data in self.market_data_list:
                    self._available_minute_fields |= set(market_data.minute_fields)
            return self._available_minute_fields

    def get_market_data(self, account_type):
        """
        Get market data by account type.
        Args:
            account_type(string): account type
        """
        if account_type == SecuritiesType.futures:
            return self.futures_market_data

    def load_current_trading_day_bars(self, current_trading_day, debug=False):
        """
        Load current trading day bars

        Args:
            current_trading_day(datetime.datetime): current trading day
            debug(boolean): debug or not
        """
        normalized_today_minutes = set()
        for market_data in self.market_data_list:
            if market_data is not None:
                market_bar_time = market_data.load_current_trading_day_bars(current_trading_day, debug=debug)
                normalized_today_minutes |= market_bar_time
                self.minute_bars_loaded_days = market_data.minute_bars_loaded_days or self.minute_bars_loaded_days
        today_minutes = sorted(normalized_today_minutes)
        if '21:00' in today_minutes:
            begin_index = today_minutes.index('21:00')
            today_minutes = today_minutes[begin_index:] + today_minutes[:begin_index]
        current_minute_bar_map = {current_trading_day.strftime('%Y-%m-%d'): today_minutes}
        self.minute_bar_map.update(current_minute_bar_map)

    def back_fill_rt_bar_times(self, date, bar_time_list):
        """
        模拟交易场景填充实时分钟bar

        Args:
            date(datetime.datetime): date
            bar_time_list(list): list of barTime
        """
        date_string = date.strftime('%Y-%m-%d')
        if date_string not in self.minute_bar_map:
            return
        if bar_time_list:
            self.minute_bar_map[date_string].extend(bar_time_list)

    def prepare_daily_cache(self, symbols, end_date, time_range, fields=TRADE_ESSENTIAL_MINUTE_BAR_FIELDS):
        """
        准备分钟线cache数据

        Args:
            symbols(list): symbol list
            end_date(string): end date
            time_range(int): time range
            fields(list): field list
        """
        daily_cache_data = {e: {} for e in ['tas', 'sat', 'ast']}
        for market_data in self.market_data_list:
            selected_universe = self.asset_service.filter_symbols(asset_type=market_data.asset_type, symbols=symbols)
            selected_universe = list(selected_universe)
            if not len(selected_universe):
                continue
            ast_array, time_bars = market_data.slice(selected_universe, fields, end_date, freq='d',
                                                     time_range=time_range, rtype='array',
                                                     cached_trading_days=self.calendar_service.cache_all_trading_days,
                                                     no_stylish=True)
            adj_data_dict, time_bars = \
                market_data.adjust(ast_array, selected_universe, time_bars, f_adj=None, s_adj='pre_adj', freq='d')
            for k, cache_item in daily_cache_data.iteritems():
                raw_data_dict = ast_array if k == 'tas' else adj_data_dict
                daily_cache_data[k] = \
                    _append_data(cache_item, _ast_stylish(raw_data_dict, selected_universe, time_bars,
                                                          fields, k, rtype='array'), k, rtype='array')
        return daily_cache_data

    def prepare_minute_cache(self, symbols, end_date, time_range, fields=TRADE_ESSENTIAL_MINUTE_BAR_FIELDS):
        """
        准备分钟线cache数据

        Args:
            symbols(list): symbol list
            end_date(string): end date
            time_range(int): time range
            fields(list): field list
        """
        # minute_cache_data = {e: {} for e in ['tas', 'sat', 'ast']}
        minute_cache_data = {e: {} for e in ['tas', 'sat']}
        for market_data in self.market_data_list:
            selected_universe = self.asset_service.filter_symbols(asset_type=market_data.asset_type, symbols=symbols)
            selected_universe = list(selected_universe)
            selected_universe = market_data._valid_symbols(selected_universe)
            if not len(selected_universe):
                continue
            ast_array, time_bars = market_data.slice(selected_universe, fields, end_date, freq='m',
                                                     time_range=time_range, rtype='array',
                                                     cached_trading_days=self.calendar_service.cache_all_trading_days,
                                                     no_stylish=True)
            adj_data_dict, time_bars = \
                market_data.adjust(ast_array, selected_universe, time_bars, f_adj=None, s_adj='pre_adj', freq='m')
            for k, cache_item in minute_cache_data.iteritems():
                raw_data_dict = ast_array if k == 'tas' else adj_data_dict
                minute_cache_data[k] = \
                    _append_data(cache_item, _ast_stylish(raw_data_dict, selected_universe, time_bars,
                                                          fields, k, rtype='array'), k, rtype='array')
        return minute_cache_data


class MarketData(object):
    """
    行情内容包装类

    Attributes:
        * daily_bars: 日线数据，默认格式为ast的dict of DataFrame
        * minute_bars: 分钟线数据，默认格式为ast的dict of dict of ndarray
    """

    def __init__(self, universe, daily_fields, minute_fields, daily_bars_loader, minute_bars_loader,
                 daily_bars_check_field='closePrice', minute_bars_check_field='closePrice', asset_type=None):
        self.universe = universe
        self.asset_type = asset_type
        self.factor_bars = dict()
        self.daily_bars = dict()

        self.daily_fields = daily_fields
        self.dividends = None
        self.allot = None
        self._daily_bar_check_field = daily_bars_check_field
        self._daily_bars_loader = daily_bars_loader
        self._daily_bars_loaded_days = list()

        self.minute_bars = dict()
        self.minute_fields = minute_fields
        self._minute_bars_check_field = minute_bars_check_field
        self._minute_bars_loader = minute_bars_loader
        self._minute_bars_expanded = dict()
        self._minute_bars_loaded_days = list()
        self._load_multi_freq_data = None

    @property
    def daily_bars_loaded_days(self):
        """
        Daily bars loaded days.
        """
        return self._daily_bars_loaded_days

    @property
    def minute_bars_loaded_days(self):
        """
        Minute bars loaded days.
        """
        return self._minute_bars_loaded_days

    def rolling_load_daily_data(self, trading_days, max_cache_days=None, asset_service=None):
        """
        MarketData的日行情加载方法

        Args:
            trading_days(list of datetime): 需加载的交易日，backtest中已含max_window_history
            max_cache_days(int): daily_bars最大加载的交易天数，默认加载全部交易日
            asset_service()
        """
        self.daily_bars = _rolling_load_data(self.daily_bars, trading_days, self.universe, max_cache_days,
                                             self._daily_bars_loader, self.daily_fields)
        self._daily_bars_loaded_days = [datetime.datetime.strptime(td, '%Y-%m-%d')
                                        for td in self.daily_bars[self._daily_bar_check_field].index]
        self._load_dividends(trading_days)
        self._load_allots(trading_days)

    def rolling_load_minute_data(self, trading_days, max_cache_days):
        """
        MarketData滚动加载分钟线数据, 如cache_minute_bars则展开分钟线行情数据

        Args:
            trading_days(list of datetime): 需加载分钟线的交易日
            max_cache_days: minute_bars中最大加载的交易日

        Returns:
            dict of DataFrame (ast格式)，当前增量加载完成之后的分钟线数据
        """
        if self._daily_bar_check_field not in self.daily_bars:
            raise AttributeError('Exception in "MarketData.rolling_load_minute_data": '
                                 'daily data must be loaded before rolling load minute data')
        if not set(trading_days) <= set(self._daily_bars_loaded_days):
            raise AttributeError('Exception in "MarketData.rolling_load_minute_data": '
                                 'minute increment load data must be in scope of daily trading data')
        self.minute_bars = _rolling_load_data(self.minute_bars, trading_days, self.universe, max_cache_days,
                                              self._minute_bars_loader, self.minute_fields)
        self._minute_bars_loaded_days = [datetime.datetime.strptime(td, '%Y-%m-%d')
                                         for td in self.minute_bars[self._minute_bars_check_field].index]
        return self.minute_bars

    def load_current_trading_day_bars(self, current_trading_day, debug=False):
        """
        MarketData加载当日已聚合分钟线
        Args:
            current_trading_day(datetime.datetime): 交易日
            debug(boolean): debug or not

        Returns(set):
            该MarketData的分钟BarTime
        """
        if not debug:
            current_bars = _load_current_bars_by_now(current_trading_day, self.universe,
                                                     fields=self.minute_fields,
                                                     load_func=self._current_trading_day_bars_loader)
        else:
            # using default empty values
            current_bars = dict()
            for field in self.minute_fields:
                default_array = np.array([], dtype='S') if field in ['barTime', 'tradeTime'] else np.array([])
                values = {symbol: default_array for symbol in self.universe}
                frame = pd.DataFrame.from_dict({current_trading_day.strftime('%Y-%m-%d'): values}, orient='index')
                current_bars[field] = frame
        for field in current_bars:
            self.minute_bars[field] = self.minute_bars[field].append(current_bars[field])
        self._minute_bars_loaded_days = [datetime.datetime.strptime(td, '%Y-%m-%d')
                                         for td in self.minute_bars[self._minute_bars_check_field].index]
        normalized_bar_time = set()
        if self.asset_type == AssetType.FUTURES:
            for bar_array in current_bars['barTime'].loc[current_trading_day.strftime('%Y-%m-%d')].tolist():
                normalized_bar_time |= set(bar_array)
        else:
            universal_last_minute = max(current_bars['barTime'].loc[current_trading_day.strftime('%Y-%m-%d')].apply(
                lambda x: x[-1] if x.size > 0 else ''))
            if universal_last_minute:
                raise NotImplementedError
        return normalized_bar_time

    def adjust(self, raw_data_dict, symbols, time_bars, **kwargs):
        """
        Adjust market prices

        Args:
            raw_data_dict(dict): original data
            symbols(list): symbol list
            time_bars(list): time_bar list
            **kwargs: key-value parameters
        """
        return raw_data_dict, time_bars

    def slice(self, symbols, fields, end_date=None, freq='d', style='ast', time_range=1, rtype='frame',
              f_adj=None, s_adj=None, cached_trading_days=None, prepare_dates=None, **options):
        """
        行情Panel数据的筛选

        Args:
            symbols(list of symbol): 需要筛选的symbols列表
            fields(list of str): 需要筛选的字段列表
            end_date(datetime.datetime): 需要获取的行情结束时间
            freq('d' or 'm'): 需要获取的行情数据频率，'d'代表日线，'m'代表分钟线
            style('ast', 'sat' or 'tas'): 返回数据Panel的层次样式顺序（field->column->index），其中'a'表示attribute，'s'表示symbol，'t'表示time，默认'ast'
            time_range(int): 切割end_date前time_range个交易日
            rtype('frame' or 'array'): 返回的Panel数据的格式，frame表示dict of DataFrame, dict表示dict of array
            f_adj(string): futures adj type
            s_adj(string): stock adj type
            cached_trading_days(list of str time): 所有交易日缓存
            prepare_dates(list of datetime): 为了完成slice，需要确保分钟线已经加载并展开的日期
        Returns:
            dict, 格式视style与rtype参数输入
        -------
        """
        end_time_str = end_date.strftime('%Y-%m-%d')
        self._check_time_range(end_date, freq)
        symbols = self._valid_symbols(symbols)
        fields = self._valid_fields(fields, freq)
        check_attribute = self._daily_bar_check_field if freq == 'd' else self._minute_bars_check_field
        if freq == 'd':
            data = self.daily_bars
        elif freq == 'm':
            data = self.minute_bars
        else:
            raise AttributeError('Exception in "MarketData.slice": unknown data slice query')
        raw_data_dict, time_bars = _ast_slice(data, symbols, end_time_str=end_time_str, fields=fields,
                                              check_attribute=check_attribute, time_range=time_range,
                                              cached_trading_days=cached_trading_days, **options)
        if options.get('no_stylish'):
            return raw_data_dict, time_bars
        adj_data_dict, time_bars = self.adjust(raw_data_dict, symbols, time_bars, f_adj=f_adj, s_adj=s_adj, freq=freq)
        return _ast_stylish(adj_data_dict, symbols, time_bars, fields, style, rtype=rtype)

    def _valid_symbols(self, symbols):
        """
        slice的helper函数，过滤valid_symbol
        """
        valid_symbols = self.daily_bars[self._daily_bar_check_field].columns
        return [symbol for symbol in symbols if symbol in valid_symbols]

    def _valid_fields(self, fields, freq='d'):
        """
        slice的helper函数，过滤valid_fields
        """
        fields = fields if isinstance(fields, list) else [fields]
        if freq == 'd':
                return list(set(fields) & set(self.daily_fields))
        elif freq == 'm' or MULTI_FREQ_PATTERN.match(freq):
            return list(set(fields) & set(self.minute_fields))

    def _load_dividends(self, *args, **kwargs):
        """
        Load dividends.
        Args:
            *args: list parameters
            **kwargs: key-value parameters
        """
        return

    def _load_allots(self, *args, **kwargs):
        """
        Load allots.
        Args:
            *args: list parameters
            **kwargs: key-value parameters
        """
        return

    def _check_time_range(self, end_date, freq):
        """
        检查slice时end_date和freq是否合法
        """
        valid_trading_days = []
        if freq == 'm' or MULTI_FREQ_PATTERN.match(freq):
            valid_trading_days = self._minute_bars_loaded_days
        elif freq == 'd':
            valid_trading_days = self._daily_bars_loaded_days
        return valid_trading_days[0] <= end_date <= valid_trading_days[-1]


class FuturesMarketData(MarketData):
    """
    Futures market data.
    """

    def __init__(self, futures_universe):
        """
        Args:
            futures_universe: set of stock symbol, 如：set(['IFM0', 'HCM0'])
        """
        super(FuturesMarketData, self).__init__(futures_universe,
                                                FUTURES_DAILY_FIELDS,
                                                FUTURES_MINUTE_FIELDS,
                                                self._daily_data_loader,
                                                self._minute_data_loader,
                                                asset_type=AssetType.FUTURES)
        self._prev_clearing_date_map = dict()
        self.continuous_fq_factors = {}

    def adjust(self, raw_data_dict, symbols, time_bars, f_adj=None, freq='d', **kwargs):
        """
        Futures adjustment.
        Args:
            raw_data_dict(dict): raw data dict
            symbols(string): symbol
            time_bars(list): time bar list
            f_adj(string): f_adj
            freq(string): frequency
        """
        data_dict = raw_data_dict
        adj_info = self.continuous_fq_factors.get(f_adj, None)
        adj_columns = {x: symbols.index(x) for x in symbols if CONTINUOUS_FUTURES_PATTERN.match(x)}
        valid_keys = ['closePrice', 'openPrice', 'highPrice', 'lowPrice', 'settlementPrice', 'preSettlementPrice']
        adj_keys = list(set(valid_keys) & set(raw_data_dict.keys()))
        adj_func = adj_func_choose(f_adj)
        if adj_info and adj_func and adj_columns and adj_keys:
            adj_matrix = adj_matrix_choose(f_adj, (len(time_bars), len(symbols)))
            for column, column_index in adj_columns.iteritems():
                adj_matrix[:, column_index] = adj_func(time_bars, adj_info[column])
            for key in adj_keys:
                data_dict[key] = adj_operator(data_dict[key], adj_matrix, f_adj)
        return data_dict, time_bars

    def rolling_load_daily_data(self, trading_days, max_cache_days=None, asset_service=None):
        """
        FuturesMarketData的rolling_load_daily_data，一次全部加载完整的trading_days。

        Args:
            trading_days(list of datetime): 需加载的交易日，backtest中已含max_window_history
            max_cache_days(int): daily_bars最大加载的交易天数，默认加载全部交易日
        """
        if len(trading_days) == 0:
            return
        MarketData.rolling_load_daily_data(self, trading_days, max_cache_days)
        self._prev_clearing_date_map = dict(zip(
            self._daily_bars_loaded_days, [get_previous_trading_date(trading_days[0])] + self._daily_bars_loaded_days[:-1]))
        self._prev_clearing_date_map = {key.strftime('%Y-%m-%d'): value.strftime('%Y-%m-%d')
                                        for key, value in self._prev_clearing_date_map.iteritems()}
        continuous_list = asset_service.filter_symbols(asset_type=AssetType.CONTINUOUS_FUTURES)
        # if len(continuous_list) > 0:
        #     self.calc_continuous_fq_factors(continuous_list, asset_service._artificial_switch_info)

    def rolling_load_minute_data(self, trading_days, max_cache_days):
        """
        FuturesMarketData加载压缩好的分钟线数据

        Args:
            trading_days(list of datetime): 需加载分钟线的交易日list
            max_cache_days(int): minute_bars最大加载的分钟线交易日数量

        Returns:
            dict, 压缩好的各fields分钟线行情
        """
        minute_data_compressed = MarketData.rolling_load_minute_data(self, trading_days, max_cache_days)
        return minute_data_compressed

    def calc_continuous_fq_factors(self, continuous_futures=None, artificial_switch_info=None):
        """
        计算连续合约的价差平移因子，前复权因子
        Args:
            continuous_futures(list of str): 连续合约名称列表
            artificial_switch_info(Series): 连续合约切换信息

        """
        if continuous_futures is None:
            return

        fq_add, fq_multiple = defaultdict(OrderedDict), defaultdict(OrderedDict)
        self.continuous_fq_factors.update({'add': fq_add, 'mul': fq_multiple})

        daily_close = self.daily_bars['closePrice']
        for date in artificial_switch_info.index:
            q_date = date.strftime('%Y-%m-%d')
            if q_date not in daily_close.index:
                for continuous, switch in artificial_switch_info[date].iteritems():
                    self.continuous_fq_factors['add'][continuous][q_date] = 0
                    self.continuous_fq_factors['mul'][continuous][q_date] = 1
                continue
            index = daily_close.index.get_loc(q_date) - 1
            for continuous, switch in artificial_switch_info[date].iteritems():
                if not all(switch) or filter(lambda x: not isinstance(x, (str, unicode)), switch):
                    # 含主力合约切换信息某一项为None
                    continue
                symbol_from, symbol_to = switch
                column_from = daily_close.columns.get_loc(symbol_from)
                column_to = daily_close.columns.get_loc(symbol_to)
                data_from, data_to = daily_close.iat[index, column_from], daily_close.iat[index, column_to]
                if filter(lambda x: not x or np.isnan(x), (data_from, data_to)):
                    continue
                add = data_to - data_from
                multiple = data_to / data_from
                self.continuous_fq_factors['add'][continuous][q_date] = 0 if np.isnan(add) else add
                self.continuous_fq_factors['mul'][continuous][q_date] = 1 if np.isnan(multiple) else multiple

    def get_trade_time(self, clearing_date, minute_bar):
        """
        根据清算日期和分钟线获取对应的trade_time，主要用作expand_slice的查询时的end_time_str
        Args:
            clearing_date(datetime): 清算日期
            minute_bar(str): 分钟线，格式为HH:mm
        """
        prev_trading_day = self._prev_clearing_date_map.get(clearing_date, None)
        if prev_trading_day is None:
            raise AttributeError('Exception in "FuturesMarketData.get_trade_time": '
                                 'unknown clearing date {}'.format(clearing_date))
        if minute_bar > '16:00':
            return '{} {}'.format(prev_trading_day, minute_bar)
        else:
            return '{} {}'.format(clearing_date, minute_bar)

    @staticmethod
    def _daily_data_loader(universe, trading_days, fields):
        daily_data = load_futures_daily_data(universe, trading_days, FUTURES_DAILY_FIELDS)
        if 'turnoverVol' in fields:
            daily_data['turnoverVol'] = daily_data.get('turnoverVol', daily_data.get('volume'))
        return daily_data

    @staticmethod
    def _minute_data_loader(universe, trading_days, fields, freq='m'):
        """
        FuturesMarketData.minute_bars的具体加载函数
        """
        minute_data = load_futures_minute_data(universe, trading_days, FUTURES_MINUTE_FIELDS, freq=freq)
        if 'turnoverVol' in fields:
            minute_data['turnoverVol'] = minute_data['volume']
        return minute_data

    def _current_trading_day_bars_loader(self, current_trading_day, universe, fields):
        """
        当前交易日已聚合1分钟线bar线数据加载
        Args:
            current_trading_day:
            universe(list of set):
            fields(list)
        Returns:

        """
        # customize start_time, end_time, fields
        fields = fields or REAL_TIME_MINUTE_BAR_FIELDS
        current_date = datetime.datetime.today()
        if (current_trading_day > current_date) and (20 <= current_date.hour <= 21):
            sta_data = dict()
        else:
            sta_data = MktFutBarRTIntraDayGet(securityID=universe, start_time=None, end_time=None)
        return sta_data
