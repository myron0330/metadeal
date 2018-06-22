# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: backtest tools File
# **********************************************************************************#
import re
import bisect
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from utils.error_utils import Errors
from utils.datetime_utils import get_previous_trading_date
from .. const import (
    BASE_FUTURES_PATTERN,
    CONTINUOUS_FUTURES_PATTERN,
    TRADE_ESSENTIAL_DAILY_BAR_FIELDS,
    TRADE_ESSENTIAL_MINUTE_BAR_FIELDS,
    HISTORY_ESSENTIAL_MINUTE_BAR_FIELDS,
    MAX_CACHE_DAILY_PERIODS,
    ADJ_FACTOR,
)

MULTI_FREQ_PATTERN = re.compile('(\d+)m')
EQUITY_RT_VALUE_FIELDS =\
    ['openPrice', 'closePrice', 'highPrice', 'lowPrice', 'turnoverVol', 'turnoverValue']
EQUITY_RT_TIME_FIELDS = ['barTime', 'tradeTime']


def _tas_data_tick_expand(data, fields=None, tick_time_field='barTime'):
    """
    Expand tas data.

    Args:
        data(DataFrame): compressed data
        fields(list): field list
        tick_time_field(str): tick time field

    Returns:
        dict: key--> minute time
              value--> {'RM701': ('09:01', 2287.0, 2286.0, 2289.0, 2283.0, 656.0)}
    """
    fields = fields or ['barTime', 'closePrice']
    expanded_data = {}
    valid_symbols_list = data['symbol'][[i for i, e in enumerate(data[tick_time_field]) if e.shape is not ()]]
    for i, stk in enumerate(data['symbol']):
        if stk not in valid_symbols_list:
            continue

        for item in zip(*[data[field][i] for field in [tick_time_field] + fields]):
            item_time = item[0]
            expanded_data.setdefault(item_time, {})
            expanded_data[item_time][stk] = tuple(item[1:])
    return expanded_data


def _at_data_tick_expand(at_data):
    """
    Expand at data.

    Args:
        at_data(dict): at data

    Returns:
        dict: expanded at data
    """
    expanded_data = {}
    for attr, data in at_data.iteritems():
        if attr == 'time' or data is None:
            continue
        valid_data_list = [d for d in data if d.shape is not ()]
        if len(valid_data_list) > 0:
            expanded_data[attr] = np.concatenate(valid_data_list)
    return expanded_data


def _st_data_tick_expand(st_data):
    non_futures = [e for e in st_data if not(BASE_FUTURES_PATTERN.match(e) or CONTINUOUS_FUTURES_PATTERN.match(e))]
    return {s: np.concatenate(st_data[s]) for s in non_futures if st_data.get(s) is not None}


def _transfer_sat_to_ast(sat_data, fields):
    """
    Transfer sat data to ast data.

    Args:
        sat_data(dict): sat data
        fields(list): field list

    Returns:
        dict: at data
    """
    ast_result = {field: {} for field in fields}
    for symbol, at_dict in sat_data.iteritems():
        if BASE_FUTURES_PATTERN.match(symbol) or CONTINUOUS_FUTURES_PATTERN.match(symbol):
            continue
        for field in fields:
            ast_result[field][symbol] = sat_data[symbol][field]
    return ast_result


def _to_datetime_string(bar_time, trading_day):
    """
    Generate date time string.

    Args:
        bar_time(string): bar time
        trading_day(datetime.datetime): trading day

    Returns:
        string: date string
    """
    if bar_time.startswith('2'):
        prev_trading_day = get_previous_trading_date(trading_day)
        date = prev_trading_day
    elif bar_time[:2] < '09':
        prev_next_day = get_previous_trading_date(trading_day) + timedelta(days=1)
        date = prev_next_day
    else:
        date = trading_day
    return date.strftime('%Y-%m-%d ') + bar_time


def _concatenate_multiple_freq(at_cache, multi_rt_array, multi_time_array,
                               inplace=False, tick_time_field='tradeTime'):
    """
    Concatenate multiple frequency data.

    Args:
        at_cache(dict): current at multiple cache data
        multi_rt_array(matrix): multiple real-time data array
        multi_time_array(matrix): multiple real-time time array
        inplace(Boolean): whether to replace the latest bar
        tick_time_field(string): tick time field name
    """
    column_size = at_cache[tick_time_field].size
    total_column_size = column_size + len(multi_time_array)
    if inplace:
        total_column_size -= 1
    increment_column_size = max(total_column_size - column_size, 1)
    matrix = np.zeros((len(EQUITY_RT_VALUE_FIELDS), total_column_size))

    for _, field in enumerate(EQUITY_RT_VALUE_FIELDS):
        if inplace:
            matrix[_, :(column_size - 1)] = at_cache[field][:-1]
        else:
            matrix[_, :column_size] = at_cache[field]
    matrix[:, -increment_column_size:] = multi_rt_array.T
    for i, _ in enumerate(matrix):
        at_cache[EQUITY_RT_VALUE_FIELDS[i]] = _

    matrix_time = np.empty((len(EQUITY_RT_TIME_FIELDS), total_column_size), dtype='|S16')
    for _, field in enumerate(EQUITY_RT_TIME_FIELDS):
        if inplace:
            matrix_time[_, :(column_size - 1)] = at_cache[field][:-1]
        else:
            matrix_time[_, :column_size] = at_cache[field]
    matrix_time[:, -increment_column_size:] = multi_time_array.T
    for i, _ in enumerate(matrix_time):
        at_cache[EQUITY_RT_TIME_FIELDS[i]] = _


def _aggregate_at_multiple_data(at_minute_cache, begin_index, freq_number):
    """
    Aggregate multiple frequency bar data.

    Args:
        at_minute_cache(dict): at minute data cache
        begin_index(int): begin index
        freq_number(int): frequency

    Returns:
        dict: key -->attribute, value --> np.matrix
    """
    result = {}
    if at_minute_cache['tradeTime'][begin_index:].size == 0:
        return result
    pad_len = -at_minute_cache['tradeTime'][begin_index:].size % freq_number
    for attribute, array in at_minute_cache.iteritems():
        minute_data = array[begin_index:]
        minute_pad = np.pad(minute_data, (0, pad_len), mode='constant', constant_values=(np.nan,))
        multi_data = np.resize(minute_pad, (minute_pad.size / freq_number, freq_number))
        if attribute in ['barTime', 'closePrice', 'tradeTime']:
            data = multi_data[:, -1]
            data[-1] = array[-1]
        elif attribute == 'openPrice':
            data = multi_data[:, 0]
        elif attribute == 'highPrice':
            data = np.nanmax(multi_data, axis=1)
        elif attribute == 'lowPrice':
            data = np.nanmin(multi_data, axis=1)
        elif attribute in ['turnoverValue', 'turnoverVol']:
            data = np.nansum(multi_data, axis=1)
        else:
            raise Exception('Bad attribute in minute bar data.')
        result[attribute] = data
    return result


class MarketRoller(object):

    tas_daily_cache = None
    tas_minute_cache = None
    sat_minute_cache = None
    tas_daily_expanded_cache = None
    tas_minute_expanded_cache = None
    multi_freq_cache = {}
    multi_freq_cache_dates = {}

    def __init__(self, universe, market_service, trading_days, daily_bar_loading_rate, minute_bar_loading_rate,
                 debug=False, paper=False):
        self.universe = universe
        self.market_service = market_service
        self.trading_days = trading_days
        self.trading_days_length = len(trading_days)
        self.daily_bar_loading_rate = daily_bar_loading_rate
        self.minute_bar_loading_rate = minute_bar_loading_rate
        self.debug = debug
        self.paper = paper

    def prepare_daily_data(self, current_date, extend_loading_days=1):
        """
        依据当前时间和更新频率准备行情数据
        """
        if not self.tas_daily_cache or current_date not in self.tas_daily_cache:
            current_index = self.trading_days.index(current_date)
            offset_index = min(current_index + self.daily_bar_loading_rate - 2, len(self.trading_days) - 1)
            end_date = self.trading_days[offset_index]
            time_range = self.daily_bar_loading_rate + extend_loading_days
            self.tas_daily_cache = self.market_service.slice(symbols=self.universe,
                                                             fields=TRADE_ESSENTIAL_DAILY_BAR_FIELDS,
                                                             time_range=time_range, end_date=end_date,
                                                             freq='d', style='tas')
            sorted_dates = sorted(self.tas_daily_cache)
            dates_length = len(self.tas_daily_cache) - 1
            for index, date in enumerate(sorted_dates):
                next_index = min(index + 1, dates_length)
                next_date = sorted_dates[next_index]
                frame = self.tas_daily_cache[date]
                next_frame = self.tas_daily_cache[next_date]
                if ADJ_FACTOR in frame.columns:
                    frame[ADJ_FACTOR] = frame[ADJ_FACTOR].fillna(1)
                    if ADJ_FACTOR in next_frame.columns:
                        next_frame[ADJ_FACTOR] = next_frame[ADJ_FACTOR].fillna(1)
                    else:
                        next_frame[ADJ_FACTOR] = frame[ADJ_FACTOR]
                    frame[ADJ_FACTOR] = frame[ADJ_FACTOR] / next_frame[ADJ_FACTOR]
            self.tas_daily_cache = \
                {datetime.strptime(key, '%Y-%m-%d'): value for key, value in self.tas_daily_cache.iteritems()}
        if not self.tas_daily_expanded_cache or current_date not in self.tas_daily_expanded_cache:
            self.tas_daily_expanded_cache = \
                self.tas_daily_expanded_cache if self.tas_daily_expanded_cache is not None else dict()
            current_index = self.trading_days.index(current_date)
            start_index = max(current_index - 2, 0)
            end_index = current_index + MAX_CACHE_DAILY_PERIODS
            if end_index >= self.trading_days_length - 1:
                prepare_dates = self.trading_days[start_index:]
            else:
                prepare_dates = self.trading_days[start_index: end_index]
            for date in set(self.tas_daily_expanded_cache) - set(prepare_dates):
                if date not in prepare_dates:
                    del self.tas_daily_expanded_cache[date]

            for date in prepare_dates:
                if date in self.tas_daily_expanded_cache:
                    continue

                zipped_data = self.tas_daily_cache.get(date)
                if zipped_data is None:
                    continue
                cache_data = self.tas_daily_expanded_cache[date] = zipped_data.to_dict()
                if ADJ_FACTOR in cache_data:
                    cache_data['adjClosePrice'] = {
                        symbol: round(price * cache_data[ADJ_FACTOR][symbol], 3)
                        if price and cache_data[ADJ_FACTOR][symbol] else price
                        for symbol, price in cache_data['closePrice'].iteritems()
                    }
                else:
                    cache_data['adjClosePrice'] = cache_data['closePrice']
                cache_data['reference_price'] = cache_data['adjClosePrice']
                # 期货用结算价
                if 'settlementPrice' in self.tas_daily_expanded_cache[date]:
                    settlement_reference = self.tas_daily_expanded_cache[date]['settlementPrice']
                    settlement_dict = \
                        {symbol: float(value) for (symbol, value) in settlement_reference.iteritems()
                         if not np.isnan(value)}
                    cache_data['reference_price'].update(settlement_dict)
                # 场外基金用当日净值
                if 'nav' in self.tas_daily_expanded_cache[date]:
                    nav_reference = self.tas_daily_expanded_cache[date]['nav']
                    nav_dict = \
                        {symbol: float(value) for (symbol, value) in nav_reference.iteritems()}
                    cache_data['reference_price'].update(nav_dict)
        return self.tas_daily_expanded_cache

    def prepare_minute_data(self, current_date, extend_loading_days=1):
        """
        依据当前时间和更新频率准备行情数据
        """
        if not self.tas_minute_cache or current_date not in self.tas_minute_cache:
            self.tas_minute_cache = dict()
            current_index = self.trading_days.index(current_date)
            # 准备loading_rate天数(含当天)的minute_cache，offset_index要 - 1
            offset_index = min(current_index + self.minute_bar_loading_rate, len(self.trading_days)) - 1
            end_date = self.trading_days[offset_index]
            time_range = self.minute_bar_loading_rate + extend_loading_days
            cache_items = self.market_service.prepare_minute_cache(self.universe, end_date,
                                                                   time_range=time_range,
                                                                   fields=HISTORY_ESSENTIAL_MINUTE_BAR_FIELDS)

            sat_data = cache_items['sat']
            self.sat_minute_cache = \
                {s: _at_data_tick_expand(at_data) for (s, at_data) in sat_data.iteritems()}
            tas_data = cache_items['tas']
            for date, data in tas_data.iteritems():
                self.tas_minute_cache[datetime.strptime(date, '%Y-%m-%d')] = data
        self.tas_minute_expanded_cache = {
            current_date: _tas_data_tick_expand(self.tas_minute_cache[current_date], TRADE_ESSENTIAL_MINUTE_BAR_FIELDS)
        }
        return self.tas_minute_expanded_cache

    def back_fill_rt_data(self, current_trading_day=None, rt_data=None):
        """
        加载推送的分钟线截面数据
        Args:
            current_trading_day(datetime.datetime): 实时行情结算日
            rt_data(list): list of (barTime, symbol_bar_data)

        Returns(list):
            所加载的 barTime 列表

        """
        if not current_trading_day:
            return
        current_date = current_trading_day
        for idx in range(0, len(rt_data)):
            bar_time, bar_data = rt_data[idx]
            if self.debug:
                idx_trade_time = _to_datetime_string(bar_time, current_trading_day)
            else:
                idx_trade_date = datetime.today().strftime('%Y-%m-%d')
                if datetime.now().strftime('%H') in ['00', '01']:
                    if bar_time.startswith('2'):
                        yesterday = datetime.today() - timedelta(days=1)
                        idx_trade_date = yesterday.strftime('%Y-%m-%d')
                idx_trade_time = idx_trade_date + ' ' + bar_time

            tas_idx_bar = {}
            for symbol, at_cache in self.sat_minute_cache.iteritems():
                if symbol not in bar_data:
                    continue
                symbol_data = bar_data[symbol]
                multi_time_array = np.mat([[bar_time, idx_trade_time]])
                _concatenate_multiple_freq(at_cache, np.mat([symbol_data]), multi_time_array)

                symbol_data.extend([[bar_time, idx_trade_time]])
                tas_idx_bar.update({symbol: tuple(symbol_data)})
            minute_bar = {bar_time: tas_idx_bar}
            self.tas_minute_expanded_cache[current_date].update(minute_bar)

    def reference_price(self, date=None, minute=None):
        """
        返回传入时刻点的行情最新价格

        Args:
            date(datetime.datetime): 交易日
            minute(str): 如 '13:00'

        Returns(dict): key为symbol， value为最新价格

        """
        if minute:
            latest_date = date if date in self.tas_minute_expanded_cache else max(self.tas_minute_expanded_cache)
            minute_cache_data = self.tas_minute_expanded_cache[latest_date]
            if minute in minute_cache_data:
                minute_price = minute_cache_data[minute]
            else:
                all_minute_before = \
                    [e for e in self.market_service.minute_bar_map[latest_date.strftime('%Y-%m-%d')][::-1]
                     if (e < minute < '20:50') or ('20:50' < e < minute)]
                for time in all_minute_before:
                    minute_data = minute_cache_data.get(time)
                    if minute_data:
                        minute_price = minute_data
                        break
                else:
                    minute_price = dict()
            reference_price = {symbol: value[1] for (symbol, value) in minute_price.iteritems()}
        else:
            reference_price = self.tas_daily_expanded_cache[date]['reference_price']
        return reference_price

    def reference_return(self, date=None, minute=None):
        """
        返回传入时刻点的行情当日涨跌

        Args:
            date(datetime.datetime): 交易日
            minute(str): 如 '13:00'

        Returns(dict): key为symbol， value为当日涨跌
        """
        previous_trading_day = self.market_service.calendar_service.previous_trading_day_map[date]
        previous_ref_price = self.tas_daily_expanded_cache[previous_trading_day]['reference_price']
        if minute:
            reference_price = self.reference_price(date, minute)
        else:
            reference_price = self.tas_daily_expanded_cache[date]['reference_price']
        reference_return = {k: (v / previous_ref_price.get(k) - 1) if previous_ref_price.get(k) else None
                            for k, v in reference_price.iteritems()}
        return reference_return

    def current_price(self, symbol, date=None, minute=None, default=0.):
        """
        返回symbol在传入时刻点的当前价格

        Args:
            symbol(str): 资产符号
            date(datetime.datetime): 交易日
            minute(str): 如 '13:00'
            default(obj): default value

        Returns(float): 当前价格

        """
        if minute:
            minute_data = self.tas_minute_expanded_cache[date].get(minute)
            if minute_data and symbol in minute_data:
                return minute_data[symbol][1]
            t_date = date.strftime("%Y-%m-%d")
            all_minute_before = [e for e in self.market_service.minute_bar_map[t_date][::-1]
                                 if (e < minute < '20:50') or ('20:50' < e < minute)]
            today_minute_cache = self.tas_minute_expanded_cache[date]
            for time in all_minute_before:
                minute_data = today_minute_cache.get(time)
                if minute_data and symbol in minute_data:
                    return minute_data[symbol][1]
            return np.nan
        else:
            reference_price = self.reference_price(date).get(symbol, default)
            return reference_price

    def get_unadjusted_price(self, symbol, field, freq='d', date=None, minute=None):
        """
        Get unadjusted price: mainly for transaction
        Args:
            symbol(string): symbol
            field(string): needed field
            freq(string): frequency
            date(datetime.datetime): current date
            minute(string): current minute
        Returns:
            float: market data
        """
        assert isinstance(symbol, basestring), 'Exception in "FuturesAccount.get_transact_data": ' \
                                               'The input of symbol must be a string! '
        assert isinstance(field, basestring), 'Exception in "FuturesAccount.get_transact_data": ' \
                                              'The input of field must be a string! '
        if freq == 'd':
            cached_data = self.tas_daily_expanded_cache[date]
            market_data = cached_data.get(field, dict()).get(symbol, np.nan)
        elif freq == 'm':
            cached_data = self.tas_minute_expanded_cache[date][minute]
            essential_fields = TRADE_ESSENTIAL_MINUTE_BAR_FIELDS
            market_data = cached_data.get(symbol, [0]*len(essential_fields))[essential_fields.index(field)]
        else:
            raise ValueError('Exception in "FuturesAccount.get_transact_data": '
                             'freq must be \'d\'(daily) or \'m\'(minute)! ')
        return market_data

    def slice(self, prepare_dates, end_time, time_range, fields=None, symbols='all', style='sat', rtype='array',
              freq='m'):
        """
        对展开后的分钟线数据进行筛选获取

        Args:
            prepare_dates(list of datetime): 为了完成slice，需要确保分钟线已经加载并展开的日期
            end_time(date formatted str): 需要查询历史数据的截止时间，格式为'YYYYmmdd HH:MM'
            time_range(int): 需要查询历史数据的时间区间
            fields(list of str): 需要查询历史数据的字段列表
            symbols(string or list of string): 需要查询历史数据的符号列表
            style(sat or ast): 筛选后数据的返回样式
            rtype(dict or frame): 筛选后数据Panel的返回格式，dict表示dict of dict，frame表示dict of DataFrame
            freq(string): 'd' or 'm'

        Returns:
            dict，根据style和rtype确定样式和结构
        """
        if freq == 'm' and set(prepare_dates) > set(self.market_service.minute_bars_loaded_days):
            raise Errors.INVALID_HISTORY_END_MINUTE
        sat_fields = fields if style == 'sat' and rtype == 'array' else fields + ['tradeTime']
        with_time = False if style == 'sat' and rtype == 'frame' else True
        sat_array = self.sat_slice(prepare_dates, end_time, time_range,
                                   fields=sat_fields, symbols=symbols,
                                   with_time=with_time, freq=freq)
        if style == 'sat':
            if rtype == 'frame':
                result = {s: pd.DataFrame(at_data).set_index('tradeTime') for (s, at_data) in sat_array.iteritems()}
            else:
                result = sat_array
        elif style == 'ast':
            result = _transfer_sat_to_ast(sat_array, sat_fields)
            if rtype == 'frame':
                trade_times = result['tradeTime'].values()[0]
                for a, st_data in result.iteritems():
                    if a == 'tradeTime':
                        continue
                    result[a] = pd.DataFrame(st_data).set_index(trade_times)
                if 'tradeTime' not in fields:
                    result.pop('tradeTime')
            else:
                result['time'] = result['tradeTime']
                if 'tradeTime' not in fields:
                    result.pop('tradeTime')
        else:
            raise AttributeError('unknown slice type {} for MarketRoller'.format(style))
        return result

    def sat_slice(self, prepare_dates, end_time, time_range, fields=None, symbols='all', with_time=False, freq='m'):
        if MULTI_FREQ_PATTERN.match(freq) and freq != '1m':
            freq_cache_dates = self.multi_freq_cache_dates.get(freq)
            if any([self.debug, self.paper]) and prepare_dates[-1] == self.market_service.minute_bars_loaded_days[-1]:
                # todo: 只有1天的情况, previous_date为空
                end_date, previous_dates = prepare_dates[-1], prepare_dates[:-1]
                # 检查之前交易日是否完备， 否则加载
                if not (freq_cache_dates and set(previous_dates) <= set(freq_cache_dates)):
                    previous_at_data = self.market_service.slice(
                        symbols='all', fields=HISTORY_ESSENTIAL_MINUTE_BAR_FIELDS,
                        freq=freq, style='sat', prepare_dates=previous_dates,
                        end_date=previous_dates[-1], time_range=len(previous_dates),
                        rtype='array', s_adj='pre_adj')
                    previous_data = {s: _at_data_tick_expand(at_data) for (s, at_data) in previous_at_data.iteritems()}
                    self.multi_freq_cache[freq] = previous_data
                    self.multi_freq_cache_dates[freq] = previous_dates
                # 更新当日 multi freq cache
                self._multiple_freq_refresh(freq)
                sat_array_data = self.multi_freq_cache[freq]
            else:
                if freq_cache_dates and set(prepare_dates) <= set(freq_cache_dates):
                    sat_array_data = self.multi_freq_cache[freq]
                else:
                    multi_freq_data = self.market_service.slice(
                        symbols='all', fields=HISTORY_ESSENTIAL_MINUTE_BAR_FIELDS,
                        freq=freq, style='sat', prepare_dates=prepare_dates,
                        end_date=prepare_dates[-1], time_range=len(prepare_dates),
                        rtype='array', s_adj='pre_adj')
                    sat_array_data = {s: _at_data_tick_expand(at_data) for (s, at_data) in multi_freq_data.iteritems()}
                    self.multi_freq_cache[freq] = sat_array_data
                    self.multi_freq_cache_dates[freq] = prepare_dates
        else:
            sat_array_data = self.sat_minute_cache
        result = {}
        symbols = sat_array_data.keys() if symbols is None or symbols == 'all' else symbols
        for symbol in symbols:
            at_data = sat_array_data[symbol]
            if len(at_data) == 0:
                continue
            end_idx = bisect.bisect_right(at_data['tradeTime'], end_time)
            local_fields = at_data.keys() if fields is None else list(set(fields) & set(at_data.keys()))
            st_result = {a: at_data[a][end_idx - time_range: end_idx] for a in local_fields}
            if with_time:
                st_result['time'] = at_data['tradeTime'][end_idx - time_range: end_idx]
            result[symbol] = st_result
        return result

    def _multiple_freq_refresh(self, freq, tick_time_field='tradeTime'):
        """
        Refresh multiple frequency data.

        Args:
            freq(string): frequency
            tick_time_field(string): tick time field
        """
        prev_trading_day = self.market_service.minute_bars_loaded_days[-2]
        ever_begin_time = prev_trading_day.strftime('%Y-%m-%d 20:59')
        for symbol, at_cache in self.sat_minute_cache.iteritems():
            multiple_at_cache = self.multi_freq_cache.get(freq, {}).get(symbol, {})
            if multiple_at_cache and multiple_at_cache[tick_time_field].size > 0:
                last_multi_time = multiple_at_cache[tick_time_field][-1]
            else:
                last_multi_time = ever_begin_time
            freq_number = int(freq[:-1])
            end_date_idx = bisect.bisect_right(at_cache[tick_time_field], ever_begin_time)
            multi_minutes = at_cache[tick_time_field][end_date_idx:][0::freq_number]
            multi_idx = bisect.bisect_right(multi_minutes, last_multi_time)
            if multi_idx == 0:
                multi_rt_array = np.mat([[at_cache[e][end_date_idx]for e in EQUITY_RT_VALUE_FIELDS]])
                multi_time_array = np.mat([
                    [at_cache['barTime'][end_date_idx], at_cache[tick_time_field][end_date_idx]]])
                _concatenate_multiple_freq(multiple_at_cache, multi_rt_array, multi_time_array)
                last_multi_time = multiple_at_cache[tick_time_field][-1]
                multi_idx = 1

            begin_minute = multi_minutes[multi_idx - 1]
            begin_idx = bisect.bisect_right(at_cache[tick_time_field], begin_minute)
            at_data_added = _aggregate_at_multiple_data(at_cache, begin_idx, freq_number)
            if at_data_added:
                multi_rt_array = np.mat([at_data_added[e] for e in EQUITY_RT_VALUE_FIELDS]).T
                multi_time_array = np.mat(
                    [at_data_added['barTime'], at_data_added[tick_time_field]]).T
                inplace = begin_minute != last_multi_time
                _concatenate_multiple_freq(multiple_at_cache, multi_rt_array, multi_time_array, inplace=inplace)
