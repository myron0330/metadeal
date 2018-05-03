# -*- coding: utf-8 -*-
import re
import logging
import datetime
import pandas as pd
from utils.error_utils import Errors
from utils.datetime_utils import normalize_date
from . base_service import ServiceInterface
from .. trade.cost import Commission, Slippage
from .. core.pattern import (
    STOCK_PATTERN,
    BASE_FUTURES_PATTERN,
    CONTINUOUS_FUTURES_PATTERN,
    INDEX_PATTERN,
    FUND_PATTERN,
    XZCE_FUTURES_PATTERN,
    OPTION_PATTERN,
    DIGITAL_CURRENCY_PATTERN
)


class AssetType(object):

    BASE_FUTURES = 'base_futures'
    CONTINUOUS_FUTURES = 'continuous_futures'
    DIGITAL_CURRENCY = 'digital_currency'

    FUTURES = [BASE_FUTURES, CONTINUOUS_FUTURES]
    ALL = FUTURES


class AssetInfo(object):
    """
    Asset info.

    Attributes:
        symbol(str): security ID
            - base_futures: eg. CU1601, IF1510
            - continuous_futures: eg. CUM0, IFM0
        asset_type(str or list): asset type
        list_date(datetime.datetime): list date
        last_date(datetime.datetime): last date
        other_symbols(list of str): other symbols
    """

    def __init__(self,
                 sec_id=None, symbol=None, exchange=None, name=None, asset_type=None, list_date=None,
                 last_date=None, other_symbols=None):
        self.sec_id = sec_id
        self.symbol = symbol
        self.exchange = exchange
        self.name = name
        self.asset_type = asset_type
        self.list_date = list_date
        self.last_date = last_date
        self.other_symbols = list() if other_symbols is None else other_symbols

    def is_active_within(self, start=None, end=None, exclude_list_date=False, exclude_last_date=False):
        """
        Active or not within a specific period.
        Args:
            start(datetime.datetime): start time
            end(datetime.datetime): end time
            exclude_list_date(boolean): whether or not exclude list date
            exclude_last_date(boolean): whether or not exclude last date

        Returns:
            bool

        """
        if self.list_date is not None and end is not None and (self.list_date >= end if exclude_list_date
                                                               else self.list_date > end):
            return False
        if self.last_date is not None and start is not None and (self.last_date <= start if exclude_last_date
                                                                  else self.last_date < start):
            return False
        return True


def _str_date(time):
    """
    Transfer date to str.

    Args:
        time(datetime.datetime): time input.
    """
    return str(time.date()) if time else None


def _encoding_string(string):
    """
    Encoding string to str.
    """
    return string.encode('utf-8') if isinstance(string, unicode) else string


def _get_date(date_str, date_pattern='%Y-%m-%d'):
    if pd.isnull(date_str) or date_str is None:
        return None
    else:
        return datetime.datetime.strptime(date_str, date_pattern)


def _normalize_zce_symbol_by_date(symbol, target_date):
    """
    Normalize zce symbol according to a target date.

    Args:
        symbol(string): future symbol
        target_date(datetime.datetime): target date

    Returns:
        string: transferred symbol
    """
    match = XZCE_FUTURES_PATTERN.match(symbol)
    return match.group(1) + target_date.strftime('%Y')[2] + match.group(2) if match else symbol


def _normalize_zce_symbol_by_period(symbol, start, end):
    """
    Normalize zce symbol according to a period.

    Args:
        symbol(string): future symbol
        start(datetime.datetime): start date
        end(datetime.datetime): end date

    Returns:
        list: transferred symbol list
    """
    if XZCE_FUTURES_PATTERN.match(symbol):
        return [symbol[:2] + str(e % 100) + symbol[2:] for e in range(start.year / 10, (end.year / 10 + 1))]
    else:
        return [symbol]


def get_future_code(symbol):
    """
    Get future code.

    Args:
        symbol(string): future symbol
    """
    if len(symbol) <= 2:
        return symbol
    symbol_object = symbol[:-2] if len(symbol) <= 4 else ''.join(re.findall(r'[A-Z]', symbol))
    return symbol_object


class FuturesAssetInfo(AssetInfo):
    """
    Futures asset info.
    """
    def __init__(self, sec_id, name, symbol, exchange, list_date=None, last_date=None, multiplier=None,
                 multiplier_unit=None, min_chg_price_num=None, min_chg_price_unit=None, price_unit=None,
                 price_valid_decimal=None, commission=None, commission_unit=None, margin_rate=None,
                 code=None, asset_type=AssetType.BASE_FUTURES):
        super(FuturesAssetInfo, self).__init__(sec_id, symbol, exchange, name, asset_type, list_date, last_date)
        self.code = get_future_code(symbol) if not code else code
        self.last_trade_date = last_date
        self.multiplier = multiplier
        self.multiplier_unit = multiplier_unit
        self.min_chg_price_num = min_chg_price_num
        self.min_chg_price_unit = min_chg_price_unit
        self._price_unit = price_unit
        self._price_valid_decimal = price_valid_decimal
        self.commission = commission
        self.commission_unit = commission_unit
        self.margin_rate = margin_rate

    @staticmethod
    def from_cached_dict(row):
        future_security = '.'.join([row['symbol'], row['exchangeCD']])
        asset_info = FuturesAssetInfo(sec_id=future_security, name=row['secShortName'], symbol=row['symbol'],
                                      exchange=row['exchangeCD'], multiplier=row['contMultNum'],
                                      multiplier_unit=row['conMultUnit'], list_date=_get_date(row['listDate'], '%Y%m%d'),
                                      last_date=_get_date(row['lastTradeDate'], '%Y%m%d'),
                                      min_chg_price_num=row['minChgPriceNum'],
                                      min_chg_price_unit=row['minChgPriceUnit'], price_unit=row['priceUnit'],
                                      price_valid_decimal=row['priceValidDecimal'],
                                      commission=row['tradeCommiNum'],
                                      commission_unit=row['tradeCommiUnit'], margin_rate=row['tradeMarginRatio'])
        if asset_info.exchange == 'XZCE':
            asset_info.other_symbols.append(_normalize_zce_symbol_by_date(asset_info.symbol, asset_info.last_trade_date))
        return asset_info

    def zce_reformat(self, check_date):
        """
        ZCE reformat.

        Args:
            check_date(datetime.datetime): check date
        """
        if self.exchange == 'XZCE':
            if _normalize_zce_symbol_by_date(self.symbol, check_date) not in self.other_symbols:
                transfer_name = _normalize_zce_symbol_by_date(self.symbol, self.last_trade_date)
                self.other_symbols.remove(transfer_name)
                self.symbol = transfer_name

    def get_trade_params(self, custom_properties=None):
        """
        Get trade parameters.
        Args:
            custom_properties(dict): custom properties

        Returns:
            tuple: (margin_rate, commission, multiplier, min_change_price_number, slippage)
        """
        if not custom_properties:
            custom_properties = dict()
        slippage = custom_properties.get('slippage', Slippage())
        margin_rate = custom_properties.get('margin_rate', dict()).get(
            self.code, self.margin_rate / 100.0)
        commission_rate = custom_properties.get('commission', dict()).get(self.code, None)

        if not commission_rate:
            if self.commission_unit == u'元/手' or self.commission_unit == '元/手':
                commission_rate = Commission(self.commission, self.commission, 'perShare')
            elif self.commission_unit == u'%' or self.commission_unit == '%':
                commission_rate = Commission(self.commission * 0.01, self.commission * 0.01, 'perValue')
            else:
                commission_rate = Commission()

        if not all([margin_rate, self.multiplier, self.min_chg_price_num]):
            raise Errors.DATA_NOT_AVAILABLE
        else:
            return margin_rate, commission_rate, self.multiplier, self.min_chg_price_num, slippage

    def get_limit_price(self, date):
        """
        Get limit price of a specific date.

        Args:
            date(datetime.datetime): trading day

        Returns:
            tuple: (up limit price, down limit price)
        """
        trading_day = date.strftime('%Y%m%d')
        df = MktFutLimitGet(ticker=self.symbol, beginDate=trading_day, endDate=trading_day,
                            field=['limitUpPrice', 'limitDownPrice'])
        if df.shape[0] == 0:
            err_msg = "Future limit price not found at: "+trading_day
            logging.error(err_msg)
            return float(0), float(1e6)
        else:
            return float(list(df.limitUpPrice)[-1]), float(list(df.limitDownPrice)[-1])

    def __repr__(self):
        return "<FuturesAssetInfo symbol={}, name={}, exchange={}, list_date={}, last_trade_date={}, " \
               "multiplier={}, min_chg_price_num={}, min_chg_price_unit={}>".format(
                self.symbol, _encoding_string(self.name), self.exchange, _str_date(self.list_date),
                _str_date(self.last_trade_date), self.multiplier, self.min_chg_price_num,
                _encoding_string(self.min_chg_price_unit))


class ContinuousFuturesAssetInfo(object):

    def __init__(self, sec_id=None, symbol=None, exchange=None, name=None, list_date=None,
                 last_date=None, code=None, artificial_info=None):
        self.asset_type = AssetType.CONTINUOUS_FUTURES
        self.sec_id = sec_id
        self.symbol = symbol
        self.exchange = exchange
        self.name = name
        self.list_date = list_date
        self._last_date = last_date
        self.code = code
        self.artificial_info = artificial_info
        self.other_symbols = list()

    def get_symbol(self, trade_date):
        """
        Get base future symbol according to trade date.

        Args:
            trade_date(string or datetime.datetime): query date

        Returns:
            string: base future symbol
        """
        query_date = normalize_date(trade_date).strftime('%Y%m%d')
        return self.artificial_info[query_date] if query_date in self.artificial_info.index else None

    def __repr__(self):
        return "<ContinuousFuturesAssetInfo symbol={}, exchange={}, list_date={}, last_date={}>".format(
            self.symbol, self.exchange, _str_date(self.list_date), _str_date(self._last_date))


class DigitalCurrencyAssetInfo(AssetInfo):

    def __init__(self, symbol=None, exchange=None, name=None,
                 list_date=None, last_date=None, other_symbols=None):
        super(DigitalCurrencyAssetInfo, self).__init__(sec_id=symbol, symbol=symbol, exchange=exchange,
                                                       name=name, asset_type=AssetType.DIGITAL_CURRENCY,
                                                       list_date=list_date,
                                                       last_date=last_date,
                                                       other_symbols=other_symbols)

    def __repr__(self):
        return "<DigitalCurrencyAssetInfo symbol={}, name={}, exchange={}, list_date={}>".format(
            self.symbol, _encoding_string(self.name), self.exchange, _str_date(self.list_date))


class AssetService(ServiceInterface):

    """
    Asset service.
    """
    def __init__(self):
        super(AssetService, self).__init__()
        self.known_symbol_dict = {}
        self.symbol_type_table = {}
        self.all_symbols = set()
        self.artificial_begin_date = None
        self.artificial_end_date = None
        self._artificial_switch_info = None

    def batch_load_data(self, start, end, universe=None, expand_continuous_future=False, **kwargs):
        """
        Batch load asset data.

        Args:
            start(datetime.datetime): start datetime
            end(datetime.datetime): end datetime
            universe(list of universe): universe list
            expand_continuous_future(boolean): whether to expand continuous future
            **kwargs: key-value parameters

        Returns:
            AssetService(obj): asset service
        """
        self.update_with_symbols(universe, start, end, expand_continuous_future=expand_continuous_future)

    def subset(self, start, end, universe=None, **kwargs):
        """
        Subset.
        """
        return self

    @classmethod
    def init_with_symbols(cls, symbols, start_date=None, end_date=None, expand_continuous_future=False):
        """
        Create service by parameters.

        Args:
            symbols(list): symbol list
            start_date(datetime.datetime): start date
            end_date(datetime.datetime): end date
            expand_continuous_future(boolean): whether to expand continuous future

        Returns:
            AssetService: instance.
        """
        service = cls()
        service.update_with_symbols(symbols, start_date, end_date, expand_continuous_future)
        return service

    @classmethod
    def init_with_assets(cls, base_futures=None, continuous_futures=None, start_date=None, end_date=None,
                         expand_continuous_future=False, digital_currencies=None):
        """
        Create service by asset input parameters.

        Args:
            base_futures(list): base future symbols
            continuous_futures(list): continuous future symbols
            start_date(datetime.datetime): start date
            end_date(datetime.datetime): end date
            digital_currencies(list): digital currency symbols
            expand_continuous_future(boolean): whether to expand continuous future

        Return:
            AssetService: asset service.
        """
        service = cls()
        service.update_with_asset_symbols(base_futures=base_futures, continuous_futures=continuous_futures,
                                          start_date=start_date, end_date=end_date,
                                          digital_currencies=digital_currencies,
                                          expand_continuous_future=expand_continuous_future)
        return service

    def include_asset(self, asset):
        """
        Add asset.
        Args:
            asset(obj): asset object.
        """
        assert isinstance(asset, AssetInfo), Errors.INVALID_ASSET_SYMBOL
        for symbol in asset.other_symbols or [asset.symbol]:
            self.known_symbol_dict[symbol] = asset
        self.symbol_type_table.setdefault(asset.asset_type, set())
        self.symbol_type_table[asset.asset_type].add(asset)
        self.all_symbols.add(asset.symbol)

    @staticmethod
    def all_future_assets(subset='all', start_date=None):
        """
        All future assets.

        Args:
            subset(string or list): subset
            start_date(datetime.datetime): start date

        Returns:
            list: all future asset list
        """
        result = list()
        if subset is 'all' or isinstance(subset, (list, set)) and len(subset) > 50:
            futures_asset_data_raw = get_futures_base_info(None)
        elif subset is not None and 0 < len(subset) <= 50:
            futures_asset_data_raw = get_futures_base_info(subset)
        else:
            return result
        for idx, row in futures_asset_data_raw.iterrows():
            if start_date is None or start_date.strftime("%Y-%m-%d") < row['lastTradeDate']:
                result.append(FuturesAssetInfo.from_cached_dict(row))
        return result

    @staticmethod
    def all_continuous_future_assets(continuous_future_assets, start_date, end_date):
        """
        All continuous future assets

        Args:
            continuous_future_assets(string or list): continuous future assets
            start_date(datetime.datetime): start date
            end_date(datetime.datetime): end date

        Returns:
            list: all future asset list
        """
        result = list()
        codes = list(set([get_future_code(cid) for cid in continuous_future_assets]))
        codes_info = MktFutureInfoByContractObjects(codes)
        code_artificial_info = get_futures_artificial_info(codes,
                                                           start_date.strftime('%Y%m%d'),
                                                           end_date.strftime('%Y%m%d'))
        # begin artificial_switch_info
        switches = {}
        for k, df in code_artificial_info.iteritems():
            for col in df.columns[:-1]:
                switches[k+col] = \
                    pd.concat([df[col].rename('new'), df.shift(1)[col].rename('prev')],
                              axis=1).apply(lambda x: {k + col: (x['prev'], x['new'])} if x['prev'] != x['new'] else None, axis=1)
        switch_index = pd.DataFrame(switches).index.to_datetime()
        switch_day_rows = pd.DataFrame(switches).values.tolist()
        switch_without_none = [filter(lambda a: isinstance(a, dict), x) for x in switch_day_rows]
        switch_dicts = [reduce(lambda r, d: r.update(d) or r, e, {}) for e in switch_without_none]
        switch_info = pd.Series(switch_dicts, index=switch_index)
        switch_info[switch_index[0]] = {}
        # begin continuous future assets info
        for code, artificial_info in code_artificial_info.iteritems():
            for flu in artificial_info.columns:
                if any(artificial_info[flu]):
                    symbol = code + flu
                    asset_info = ContinuousFuturesAssetInfo(
                        symbol=symbol, exchange=codes_info.at[code, 'exchangeCD'], name=symbol,
                        list_date=_get_date(codes_info.at[code, 'listDate'], date_pattern="%Y%m%d"),
                        last_date=_get_date(codes_info.at[code, 'lastTradeDate'], date_pattern="%Y%m%d"),
                        code=code, artificial_info=artificial_info[flu])
                    result.append(asset_info)
        return result, switch_info

    def _remove_continuous_assets(self):
        """
        Remove continuous_futures
        """
        if AssetType.CONTINUOUS_FUTURES in self.symbol_type_table:
            self.symbol_type_table[AssetType.CONTINUOUS_FUTURES] = set()
        for k, v in self.known_symbol_dict.iteritems():
            if isinstance(v, ContinuousFuturesAssetInfo):
                self.known_symbol_dict.pop(k)

    @staticmethod
    def all_digital_currency_assets(subset='all'):
        """
        Get all digital currency assets.

        Args:
            subset(str or list): subset of digital currency.
        """
        # todo. add digital currency asset info.
        result = [DigitalCurrencyAssetInfo(symbol=symbol) for symbol in subset]
        return result

    def update_with_symbols(self, symbols, start_date=None, end_date=None, expand_continuous_future=False):
        """
        Update according to symbols.

        Args:
            symbols(list): symbol list
            start_date(datetime.datetime): start date
            end_date(datetime.datetime): end date
            expand_continuous_future(boolean): whether to expand continuous future
        """
        base_futures = [x for x in symbols if BASE_FUTURES_PATTERN.match(x)]
        continuous_futures = [x for x in symbols if CONTINUOUS_FUTURES_PATTERN.match(x)]
        digital_currencies = [x for x in symbols if DIGITAL_CURRENCY_PATTERN.match(x)]
        self.update_with_asset_symbols(base_futures=base_futures,
                                       continuous_futures=continuous_futures,
                                       start_date=start_date, end_date=end_date,
                                       digital_currencies=digital_currencies,
                                       expand_continuous_future=expand_continuous_future)

    def update_with_asset_symbols(self, base_futures=None, continuous_futures=None,
                                  start_date=None, end_date=None, digital_currencies=None,
                                  expand_continuous_future=False):
        """
        Update according to asset symbols.

        Args:

            base_futures(list): base future symbols
            continuous_futures(list): continuous future symbols
            start_date(datetime.datetime): start date
            end_date(datetime.datetime): end date
            digital_currencies(list): digital currency symbols
            expand_continuous_future(boolean): whether to expand continuous future
        """
        original_continuous_future = self.filter_symbols(AssetType.CONTINUOUS_FUTURES)
        if len(original_continuous_future) > 0:
            end_date = end_date or self.artificial_end_date or get_end_date()
            begin_date = start_date or self.artificial_begin_date
            if not (self.artificial_begin_date <= begin_date and self.artificial_end_date >= end_date):
                continuous_futures = list(set(continuous_futures) | set(original_continuous_future))
                self._remove_continuous_assets()
        continuous_futures = list(set(continuous_futures) | (set(base_futures) if expand_continuous_future else set()))
        if continuous_futures:
            end_date = end_date or self.artificial_end_date or previous_trading_day(get_end_date())
            begin_date = start_date or self.artificial_begin_date
            end = end_date if self.artificial_end_date is None else max(end_date, self.artificial_end_date)
            start = begin_date if self.artificial_begin_date is None else min(begin_date, self.artificial_begin_date)
            continuous_assets, switch = AssetService.all_continuous_future_assets(continuous_futures, start, end)
            for f in continuous_assets:
                self.include_asset(f)
                if expand_continuous_future:
                    base_futures = [] if base_futures is None else base_futures
                    base_futures = list(set(base_futures) | set(f._artificial_info.unique()) - set([None]))
            self.artificial_begin_date = start
            self.artificial_end_date = end
            self._artificial_switch_info = switch
        if base_futures:
            begin_date = start_date or self.artificial_begin_date
            all_base_futures = list(AssetService.all_future_assets(base_futures, begin_date))
            for f in all_base_futures:
                self.include_asset(f)

        if digital_currencies:
            for i in self.all_digital_currency_assets(digital_currencies):
                self.include_asset(i)

    def expand_continuous_future_symbol(self, symbol, trading_days):
        """
        Expand continuous future symbol.
        Args:
            symbol(string): future symbol
            trading_days(list): trading days
        Returns:
            dict: {trading day}-{symbol set}
        """
        result = {}
        if symbol in self.known_symbol_dict:
            asset_info = self.known_symbol_dict.get(symbol, None)
            if not isinstance(asset_info, ContinuousFuturesAssetInfo):
                raise AttributeError('Exception in "AssetService.expand_continuous_future_symbol": '
                                     'no future symbol {} found'.format(symbol))
            for td in trading_days:
                if td.strftime('%Y%m%d') in asset_info.artificial_info.index:
                    result[td] = {symbol, asset_info.artificial_info.loc[td.strftime('%Y%m%d')]} - {None}
                else:
                    result[td] = {symbol}
        return result

    def get_asset_info(self, symbol, date=None):
        """
        Get asset info.

        Args:
            symbol(string): future symbol
            date(datetime.datetime): date time
        Returns:
            AssetInfo: instance.

        """
        if date:
            date = normalize_date(date)
        else:
            date = datetime.datetime.today()
        symbol = _normalize_zce_symbol_by_date(symbol=symbol, target_date=date)
        return self.known_symbol_dict.get(symbol)

    def filter_symbols(self, asset_type=AssetType.ALL, symbols=None):
        """
        Filter qualified symbols.

        Args:
            asset_type(string or list): asset type
            symbols(list): candidate symbols

        Returns:
            set: valid symbols
        """
        return {a.symbol for a in self.filter_assets(asset_type, symbols)}

    def filter_assets(self, asset_type=AssetType.ALL, symbols=None):
        """
        Filter qualified assets.
        Args:
            asset_type(string or list): asset type
            symbols(list): candidate symbols

        Returns:
            set: valid symbols

        """
        result = set()
        asset_types = asset_type if isinstance(asset_type, list) else [asset_type]
        for a_type in asset_types:
            if a_type in self.symbol_type_table:
                result |= self.symbol_type_table[a_type]
        if symbols is not None:
            symbols = symbols if isinstance(symbols, (list, set)) else [symbols]
            symbols = set(symbols)
            result = {a for a in result if a.symbol in symbols}
        return result

    @staticmethod
    def get_futures_objects(symbols):
        """
        Get futures objects.

        Args:
            symbols(iterable): symbols
        """
        if isinstance(symbols, basestring): symbols = symbols.split(',')
        if not symbols:
            return []
        objects = map(get_future_code, symbols)
        return list(set(objects))

    def get_asset(self, symbol, date=None):
        """
        Get asset.

        Args:
            symbol(string): symbols
            date(datetime.datetime): date time

        Returns:
            AssetInfo: instance.
        """
        if not AssetService.is_valid_symbol(symbol):
            raise Errors.INVALID_ASSET_SYMBOL
        asset = self.get_asset_info(symbol, date)
        if asset is None:
            self.update_with_symbols([symbol], expand_continuous_future=True)
            asset = self.get_asset_info(symbol, date)
        return asset

    @staticmethod
    def is_valid_symbol(symbol):
        """
        Whether the symbol is valid.

        Args:
            symbol(string): symbol
        """
        if STOCK_PATTERN.match(symbol):
            return True
        elif BASE_FUTURES_PATTERN.match(symbol) or CONTINUOUS_FUTURES_PATTERN.match(symbol):
            return True
        elif INDEX_PATTERN.match(symbol) or FUND_PATTERN.match(symbol):
            return True
        elif OPTION_PATTERN.match(symbol):
            return True
        elif DIGITAL_CURRENCY_PATTERN.match(symbol):
            return True
        else:
            return False
