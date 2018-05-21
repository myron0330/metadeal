# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: backtest tools File
# **********************************************************************************#
import re
import bisect
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from .. data.data_portal import DataPortal
from .. data import MarketService, CalendarService, AssetService, AssetType
from .. data_loader.data_api import get_last_price_name_info
from .. universe.universe import UniverseService
from .. utils.error_utils import Errors
from .. utils.datetime_utils import previous_trading_day
from .. const import (
    STOCK_PATTERN,
    BASE_FUTURES_PATTERN,
    CONTINUOUS_FUTURES_PATTERN,
    TRADE_ESSENTIAL_FIELDS_DAILY,
    TRADE_ESSENTIAL_FIELDS_MINUTE,
    HISTORY_ESSENTIAL_FIELDS_MINUTE,
    STOCK_ADJ_FIELDS,
    ADJ_FACTOR,
    MAX_CACHE_DAILY_PERIODS,
)

MULTI_FREQ_PATTERN = re.compile('(\d+)m')


def get_backtest_service_deprecated(sim_params, preload_market_service=None):
    calendar_service = CalendarService(sim_params.start, sim_params.end,
                                       max_daily_window=sim_params.max_history_window_daily)

    init_universe_list = sim_params.security_base.keys()
    if sim_params.accounts is not None:
        for account in sim_params.accounts.values():
            init_universe_list += account.position_base.keys()
    universe_service = UniverseService(sim_params.universe, sim_params.trading_days,
                                       benchmarks=sim_params.benchmarks,
                                       init_universe_list=list(set(init_universe_list)))

    asset_service = AssetService.init_with_symbols(universe_service.full_universe,
                                                   start_date=sim_params.start, end_date=sim_params.end,
                                                   expand_continuous_future=True)
    # 该方案为折衷方案，后期考虑更好的实现
    universe_service.full_universe_set |= asset_service.filter_symbols(AssetType.BASE_FUTURES)
    inactive_symbols = list(asset_service.filter_inactive_symbols(universe_service.full_universe_set,
                                                                  sim_params.start, sim_params.end))
    if len(inactive_symbols) > 0:
        print 'WARNING: these assets in universe is not active between {} and {}: {}'.format(
            sim_params.start.strftime('%Y-%m-%d'), sim_params.end.strftime('%Y-%m-%d'), ','.join(inactive_symbols))
        universe_service.remove(inactive_symbols)

    if preload_market_service:
        market_service = preload_market_service
    else:
        market_service = MarketService.create_with_service(asset_service=asset_service,
                                                           universe_service=universe_service,
                                                           calendar_service=calendar_service)
    return calendar_service, asset_service, universe_service, market_service


def get_backtest_service(sim_params, preload_market_service=None, disabled_service=None):
    data_ptl = DataPortal()
    data_ptl.batch_load_data(sim_params, disabled_service=disabled_service)
    # if preload_market_service:
    #     market_service = preload_market_service
    # else:
    #     market_service = MarketService.create_with_service(asset_service=asset_service,
    #                                                        universe_service=universe_service,
    #                                                        calendar_service=calendar_service)
    cal_service = data_ptl.calendar_service
    ast_service = data_ptl.asset_service
    univ_service = data_ptl.universe_service
    mkt_service = data_ptl.market_service
    return cal_service, ast_service, univ_service, mkt_service


def _tas_data_tick_expand(data, fields=None, tick_time_field='barTime'):
    """
    返回展开的tas分钟线数据

    Args:
        data(DataFrame): 压缩好的分钟线数据
        fields(list of attribute): 如['highPrice', 'closePrice']
        tick_time_field(str): data中的分钟列名，不建议修改

    Returns:
        dict of dict，key为分钟时点，value如{'RM701':('09:01', 2287.0, 2286.0, 2289.0, 2283.0, 656.0)}
    """
    fields = fields or ['barTime', 'closePrice']
    minute_ticks = {}
    valid_symbols_list = data['symbol'][[i for i, e in enumerate(data[tick_time_field]) if e.shape is not ()]]
    for i, stk in enumerate(data['symbol']):
        if stk not in valid_symbols_list:
            continue

        for item in zip(*[data[field][i] for field in [tick_time_field] + fields]):
            ttime = item[0]
            minute_ticks.setdefault(ttime, {})
            minute_ticks[ttime][stk] = tuple(item[1:])
        # TODO tas 格式数据应当包括t应对包括分钟
        # for field in [tick_time_field] + fields:
        #     for bar in data[tick_time_field][i]:
        #         ttime = bar
        #         index = np.where(data[tick_time_field][i] == ttime)
        #         minute_ticks.setdefault(ttime, {})
        #         if field not in minute_ticks[ttime]:
        #             minute_ticks[ttime][field] = {stk: data[field][i][index].item(0)}
        #         else:
        #             if stk not in minute_ticks[ttime][field]:
        #                 minute_ticks[ttime][field][stk] = data[field][i][index].item(0)
        #             else:
        #                 minute_ticks[ttime][field].update({stk: data[field][i][index].item(0)})
    return minute_ticks


def _at_data_tick_expand(at_data):
    expanded_tick_data = {}
    for attr, data in at_data.iteritems():
        if attr == 'time' or data is None:
            continue
        valid_data_list = [d for d in data if d.shape is not ()]
        if len(valid_data_list) > 0:
            expanded_tick_data[attr] = np.concatenate(valid_data_list)
    return expanded_tick_data


def _st_data_tick_expand(st_data):
    non_futures = [e for e in st_data if not(BASE_FUTURES_PATTERN.match(e) or CONTINUOUS_FUTURES_PATTERN.match(e))]
    return {s: np.concatenate(st_data[s]) for s in non_futures if st_data.get(s) is not None}


def _dict_sat_to_ast(sat_data, fields):
    ast_result = {field: {} for field in fields}
    for symbol, at_dict in sat_data.iteritems():
        if BASE_FUTURES_PATTERN.match(symbol) or CONTINUOUS_FUTURES_PATTERN.match(symbol):
            continue
        for field in fields:
            ast_result[field][symbol] = sat_data[symbol][field]
    return ast_result


def _map_to_date(bar_time, current_trading_day):
    """
    返回bar_time所对应的日期
    """
    if bar_time.startswith('2'):
        prev_trading_day = previous_trading_day(current_trading_day)
        date = prev_trading_day
    elif bar_time[:2] < '09':
        prev_next_day = previous_trading_day(current_trading_day) + timedelta(days=1)
        date = prev_next_day
    else:
        date = current_trading_day
    return date.strftime('%Y-%m-%d ') + bar_time


class MarketRoller(object):

    tas_daily_cache = None
    tas_minute_cache = None
    sat_minute_cache = None
    tas_daily_expanded_cache = None
    tas_minute_expanded_cache = None
    multi_freq_cache = {}
    multi_freq_cache_dates = {}

    def __init__(self, universe, market_service, trading_days, daily_bar_loading_rate, minute_bar_loading_rate,
                 debug=False):
        self.universe = universe
        self.market_service = market_service
        self.trading_days = trading_days
        self.trading_days_length = len(trading_days)
        self.daily_bar_loading_rate = daily_bar_loading_rate
        self.minute_bar_loading_rate = minute_bar_loading_rate
        self.debug = debug

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
                                                             fields=TRADE_ESSENTIAL_FIELDS_DAILY,
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
                                                                   fields=HISTORY_ESSENTIAL_FIELDS_MINUTE)

            sat_data = cache_items['sat']
            self.sat_minute_cache = \
                {s: _at_data_tick_expand(at_data) for (s, at_data) in sat_data.iteritems()}
            tas_data = cache_items['tas']
            for date, data in tas_data.iteritems():
                self.tas_minute_cache[datetime.strptime(date, '%Y-%m-%d')] = data
        self.tas_minute_expanded_cache = {
            current_date: _tas_data_tick_expand(self.tas_minute_cache[current_date], TRADE_ESSENTIAL_FIELDS_MINUTE)
        }
        return self.tas_minute_expanded_cache

    def back_fill_rt_data(self, current_trading_day=None, rt_data=None, tick_time_field='barTime'):
        """
        加载推送的分钟线截面数据
        Args:
            current_trading_day(datetime.datetime): 实时行情结算日
            rt_data(list): list of (barTime, symbol_bar_data)
            tick_time_field(basestring): 校验当前分钟线的field

        Returns(list):
            所加载的 barTime 列表

        """
        if not current_trading_day:
            return
        current_date = current_trading_day
        # current_bar_time = self.market_service.minute_bar_map[current_date.strftime('%Y-%m-%d')]
        # if len(current_bar_time) > 0:
        #     latest_bar_minute = current_bar_time[-1]
        #     # todo: errors, if then?  restart?
        #     assert latest_bar_minute in rt_minutes
        #     start_idx = rt_minutes.index(latest_bar_minute) + 1
        #     if start_idx == len(rt_minutes):
        #         return
        #     # minutes_to_append = rt_minutes[rt_minutes.index(latest_bar_minute):]
        # elif len(current_bar_time) == 0 and rt_minutes[0] in ['21:00', '09:30']:
        #     start_idx = 0
        #     # minutes_to_append = rt_minutes
        # else:
        #     # todo: 系统启动异常
        #     raise Errors.INVALID_ORDER_AMOUNT
        #
        # 多一个 turnoverValue, 补充的数据结构不一致，有风险
        equity_rt_value_fields = ['openPrice', 'closePrice', 'highPrice', 'lowPrice', 'turnoverVol', 'turnoverValue']
        equity_rt_time_fields = ['barTime', 'tradeTime']
        for idx in range(0, len(rt_data)):
            bar_time, bar_data = rt_data[idx]
            if self.debug:
                idx_trade_time = _map_to_date(bar_time, current_trading_day)
            else:
                idx_trade_date = datetime.today().strftime('%Y-%m-%d')
                if datetime.now().strftime('%H') in ['00', '01']:
                    if bar_time.startswith('2'):
                        yesterday = datetime.today() - timedelta(days=1)
                        idx_trade_date = yesterday.strftime('%Y-%m-%d')
                idx_trade_time = idx_trade_date + ' ' + bar_time
            #
            tas_idx_bar = {}
            for symbol, symbol_at_cache in self.sat_minute_cache.iteritems():
                # todo: 个别symbol没推送过来，要区分处理？？？
                if symbol not in bar_data:
                    continue
                symbol_data = bar_data[symbol]
                # current bar_time length
                column_size = symbol_at_cache[tick_time_field].size
                matrix = np.zeros((len(equity_rt_value_fields), column_size + 1))
                for _, field in enumerate(equity_rt_value_fields):
                    matrix[_, :column_size] = symbol_at_cache[field]
                matrix[:, -1] = symbol_data
                for i, _ in enumerate(matrix):
                    symbol_at_cache[equity_rt_value_fields[i]] = _

                matrix_time = np.empty((len(equity_rt_time_fields), column_size + 1), dtype='|S16')
                for _, field in enumerate(equity_rt_time_fields):
                    matrix_time[_, :column_size] = symbol_at_cache[field]
                matrix_time[:, -1] = [bar_time, idx_trade_time]
                for i, _ in enumerate(matrix_time):
                    symbol_at_cache[equity_rt_time_fields[i]] = _

                # prepare to back fill self.tas_minute_expanded_cache
                symbol_data.extend([bar_time, idx_trade_time])
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
            # minute_price = self.tas_minute_expanded_cache[date][minute]
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
            # todo. optimize minute reference return
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
            # 各品种当天最终价格
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
            essential_fields = TRADE_ESSENTIAL_FIELDS_MINUTE
            market_data = cached_data.get(symbol, [0]*len(essential_fields))[essential_fields.index(field)]
        else:
            raise ValueError('Exception in "FuturesAccount.get_transact_data": '
                             'freq must be \'d\'(daily) or \'m\'(minute)! ')
        return market_data

    def slice(self, prepare_dates, end_time, time_range, fields=None, symbols='all', style='sat', rtype='array',
              freq='m', adj=None):
        """
        对展开后的分钟线数据进行筛选获取

        Args:
            prepare_dates(list of datetime): 为了完成slice，需要确保分钟线已经加载并展开的日期
            end_time(date formatted str): 需要查询历史数据的截止时间，格式为'YYYYmmdd HH:MM'
            time_range(int): 需要查询历史数据的时间区间
            fields(list of str): 需要查询历史数据的字段列表
            symbols(list of str): 需要查询历史数据的符号列表
            style(sat or ast): 筛选后数据的返回样式
            rtype(dict or frame): 筛选后数据Panel的返回格式，dict表示dict of dict，frame表示dict of DataFrame
            freq(string): 'd' or 'm'
            adj(string): 复权类型

        Returns:
            dict，根据style和rtype确定样式和结构
        """
        if freq == 'm' and set(prepare_dates) > set(self.market_service.minute_bars_loaded_days):
            raise Errors.INVALID_HISTORY_END_MINUTE
        sat_fields = fields if style == 'sat' and rtype == 'array' else fields + ['tradeTime']
        with_time = False if style == 'sat' and rtype == 'frame' else True
        sat_array = self.sat_slice(prepare_dates, end_time, time_range, sat_fields, symbols,
                                   with_time=with_time, freq=freq)
        if style == 'sat':
            if rtype == 'frame':
                result = {s: pd.DataFrame(at_data).set_index('tradeTime') for (s, at_data) in sat_array.iteritems()}
            else:
                result = sat_array
        elif style == 'ast':
            result = _dict_sat_to_ast(sat_array, sat_fields)
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
            if freq_cache_dates and set(prepare_dates) <= set(freq_cache_dates):
                sat_array_data = self.multi_freq_cache[freq]
            else:
                multi_freq_data = self.market_service.slice(
                    symbols='all', fields=HISTORY_ESSENTIAL_FIELDS_MINUTE,
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
