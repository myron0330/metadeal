# -*- coding: utf-8 -*-
import bisect
from datetime import datetime
from utils.datetime_utils import (
    get_trading_days,
    get_direct_trading_day,
    get_end_date
)
from . base_service import ServiceInterface
from .. const import (
    EARLIEST_DATE,
    DEFAULT_KEYWORDS
)
k

class CalendarService(ServiceInterface):
    """
    Calendar service
    """
    def __init__(self, start=None, end=None, max_daily_window=None):
        super(CalendarService, self).__init__()
        self.start = start
        self.end = end
        self.max_daily_window = max_daily_window
        self._trading_days = list()
        self._all_trading_days = []
        self._all_trading_days_index = None
        self._previous_trading_day_map = {}
        self._cache_all_trading_days = None
        self._cache_all_trading_days_dt = None

    def batch_load_data(self, start, end, universe=None,
                        max_history_window=DEFAULT_KEYWORDS['max_history_window'][0], **kwargs):
        """
        Batch load calendar data.
        Args:
            start(datetime.datetime): start datetime
            end(datetime.datetime): end datetime
            universe(list of universe): universe list
            max_history_window(int): max history window in daily
            **kwargs: key-value parameters

        Returns:
            CalendarService: with loaded data.
        """
        self.start = start
        self.end = end
        self.max_daily_window = max_history_window
        self._trading_days = get_trading_days(start, end)
        if max_history_window is not None:
            cache_start_date = get_direct_trading_day(start, max_history_window, forward=False)
            self._all_trading_days = get_trading_days(cache_start_date, end)
        else:
            self._all_trading_days = self._trading_days
        self._cache_all_trading_days_dt = get_trading_days(EARLIEST_DATE.strftime("%Y-%m-%d"),
                                                           get_end_date().strftime("%Y-%m-%d"))
        self._calculate_info()
        return self

    def subset(self, *args, **kwargs):
        """
        Subset the calendar service
        """
        return self

    @property
    def all_trading_days(self):
        """
        获取当前日历信息中包含的所有交易日（包含max_history_window预先获取的交易日）。
        """
        return self._all_trading_days

    @property
    def trading_days(self):
        """
        获取当前日历信息中包含的所有交易日（不包含max_history_window预先获取的交易日）
        """
        return self._trading_days

    @property
    def previous_trading_day_map(self):
        """
        Previous trading day map
        """
        return self._previous_trading_day_map

    def get_direct_trading_day_list(self, date, step, forward=True):
        """
        Get direct trading day
        Args:
            date(datetime): current date
            step(int): step
            forward(bool): forward
        Returns:
            list of datetime: 满足条件的交易日列表，包含两头，按从小到大排列
        """
        tds = self.cache_all_trading_days_dt
        date_idx = bisect.bisect_left(tds, date)
        start_index = date_idx + (step if forward else -step)
        index = min(max(start_index, 0), len(tds) - 1)

        return tds[date_idx: index+1] if forward else tds[index: date_idx+1]

    @property
    def cache_all_trading_days(self):
        """
        缓存06年之后所有交易日
        """
        return self._cache_all_trading_days

    @property
    def cache_all_trading_days_dt(self):
        """
        缓存06年之后所有交易日(datetime格式)
        """
        return self._cache_all_trading_days_dt

    def within_interval(self, end_date=None, interval=10):
        if self._all_trading_days_index is None:
            self._all_trading_days_index = dict(zip(self._all_trading_days, range(len(self._all_trading_days))))
        end_date = self.end if end_date is None else end_date
        end_date_idx = self._all_trading_days_index[end_date]
        return self.all_trading_days[end_date_idx-interval+1:end_date_idx+1]

    def within(self, start_date=None, end_date=None):
        """
        筛选出calendar中在start_date与end_date之间的交易日
        Args:
            start_date(datetime): 筛选的开始时间
            end_date(datetime): 筛选的结束时间
        """
        start_date = self.start if start_date is None else start_date
        end_date = self.end if end_date is None else end_date
        return [td for td in self.trading_days if start_date <= td <= end_date]

    def get_trade_time(self, clearing_date, minute_bar):
        """
        根据清算日期和分钟线获取对应的trade_time，主要用作expand_slice的查询时的end_time_str
        Args:
            clearing_date(datetime): 清算日期
            minute_bar(str): 分钟线，格式为HH:mm
        """
        prev_trading_day = self._previous_trading_day_map.get(clearing_date, None)
        if prev_trading_day is None:
            raise AttributeError('unknown clearing date {}'.format(clearing_date))
        if minute_bar > '16:00':
            return '{} {}'.format(prev_trading_day.strftime('%Y-%m-%d'), minute_bar)
        else:
            return '{} {}'.format(clearing_date.strftime('%Y-%m-%d'), minute_bar)

    def get_direct_trading_day(self, date, step, forward=True):
        """
        Get direct trading day
        Args:
            date: current date
            step: step
            forward: forward

        Returns:
            date: datetime
        """
        tds = self._all_trading_days
        date_idx = bisect.bisect_left(tds, date)
        start_index = date_idx + (step if forward else -step)
        index = min(max(start_index, 0), len(tds) - 1)
        return tds[index]

    def _calculate_info(self):
        self._all_trading_days_index = dict(zip(self._all_trading_days, range(len(self._all_trading_days))))
        self._cache_all_trading_days = map(lambda x: x.strftime("%Y-%m-%d"), self._cache_all_trading_days_dt)
        self._previous_trading_day_map = dict(zip(self._all_trading_days[1:], self._all_trading_days[:-1]))
