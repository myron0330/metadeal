# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
from utils.datetime_utils import *
from utils.error_utils import Errors
from utils.dict_utils import DefaultDict


class TradingScheduler(object):
    """
    Trading scheduler.
    """
    def __init__(self, start, end, freq='d', refresh_rate_d=1, refresh_rate_m=None,
                 max_history_window_d=30, max_history_window_m=5,
                 calendar_service=None, **kwargs):
        self.freq = freq
        self.refresh_rate_d = refresh_rate_d
        self.refresh_rate_m = refresh_rate_m
        self.max_history_window_d = max_history_window_d
        self.max_history_window_m = max_history_window_m
        self.calendar_service = calendar_service
        self.trading_days_bt = self.calendar_service.within(start, end)
        self.start, self.end = self.trading_days_bt[0], self.trading_days_bt[-1]
        self.history_loading_window_d = self.max_history_window_d + 1
        self.history_loading_window_m = max(self.max_history_window_m / 240, 1) + 1
        self.trading_days_daily_window = \
            self.calendar_service.within_interval(end_date=self.start,
                                                  interval=self.history_loading_window_d)[:-1]
        self.trading_days_minute_window = \
            self.calendar_service.within_interval(end_date=self.start,
                                                  interval=self.history_loading_window_m)[:-1]

        self.trading_days_for_window = max(self.trading_days_daily_window, self.trading_days_minute_window)
        self.trading_days_bt_idx = {v: k for (k, v) in enumerate(self.trading_days_bt)}
        self.trading_days_for_window_idx = {v: k for (k, v) in enumerate(self.trading_days_for_window)}

        self.minute_loading_signal = 0

        self._trigger_days = []
        self._rolling_load_range_d = dict()
        self._rolling_load_range_m = dict()
        self._minute_bars_loading_events = DefaultDict(list)

    def prepare(self, daily_loading_rate=60, minute_loading_rate=5):
        """
        Args:
            daily_loading_rate(int): daily loading rate
            minute_loading_rate(int): minute loading rate
        """
        self._prepare_trigger_days_d()
        if self.freq == 'm':
            self._rolling_load_range_m = \
                self._prepare_rolling_load_ranges(loading_rate=minute_loading_rate, freq='m')
        if self.freq == 'd':
            self._rolling_load_range_d =\
                self._prepare_rolling_load_ranges(loading_rate=daily_loading_rate, freq='d')

    def _prepare_trigger_days_d(self):
        """
        Prepare trigger days daily.
        """
        if isinstance(self.refresh_rate_d, int):
            trigger_days = self.trading_days_bt[::self.refresh_rate_d]
        elif isinstance(self.refresh_rate_d, list):
            for date in self.refresh_rate_d:
                if not isinstance(date, datetime):
                    raise Errors.INVALID_SELF_DEFINED_DATE_TYPE
                if date < self.trading_days_bt[0] or date > self.trading_days_bt[-1]:
                    raise Errors.INVALID_SELF_DEFINED_DATE_VALUE
            trigger_days = self.refresh_rate_d
        else:
            raise Errors.INVALID_REFRESH_RATE
        if not trigger_days:
            raise Errors.INVALID_TRIGGER_DAYS
        self._trigger_days = trigger_days

    def trigger_days(self):
        """
        Get trigger days list.
        """
        return self._trigger_days

    def trigger_minutes(self, minutes):
        """
        Calculate trigger minutes according to minutes and refresh_rate.

        Args:
            minutes(list): minute list.

        Returns:
            list: trigger minutes.
        """
        if isinstance(self.refresh_rate_m, int):
            trigger_minutes = minutes[::self.refresh_rate_m]
        elif isinstance(self.refresh_rate_m, list):
            trigger_minutes = self.refresh_rate_m
        else:
            raise Errors.INVALID_REFRESH_RATE
        return trigger_minutes

    def previous_date(self, date):
        """
        Get previous date.

        Args:
            date(datetime.datetime): date

        Returns:
            datetime.datetime: previous date
        """
        bt_idx = self.trading_days_bt_idx.get(date)
        if bt_idx is not None:
            return self.trading_days_bt[bt_idx - 1] if bt_idx > 0 else self.trading_days_for_window[-1]
        window_idx = self.trading_days_for_window_idx.get(date)
        if window_idx is not None:
            return self.trading_days_for_window[window_idx - 1] if window_idx > 0 else get_previous_trading_date(date)

    def is_trigger_day(self, date):
        """
        Judge whether date is trigger day.

        Args:
            date(datetime.datetime): date

        Returns:
            boolean: True or False
        """
        return date in self._trigger_days

    def _prepare_rolling_load_ranges(self, loading_rate=5, freq='d'):
        """
        Prepare rolling load range

        Args:
            loading_rate(int): loading rate
        """
        all_trading_days = self.trading_days(include_max_history=True, freq=freq)
        all_trading_days_idx = {v: k for (k, v) in enumerate(all_trading_days)}
        end_index = 0
        rolling_load_range = dict()
        for index, date in enumerate(self._trigger_days):
            date_index = all_trading_days_idx[date]
            if index == 0:
                rolling_load_range[date] = all_trading_days[:date_index+1]
                end_index = date_index + 1
            else:
                if date_index >= end_index:
                    begin_index = max(end_index, (date_index - self.history_loading_window_m))
                    end_index = begin_index + loading_rate
                    rolling_load_range[date] = all_trading_days[begin_index:end_index]
        return rolling_load_range

    def rolling_load_ranges_daily(self, date):
        """
        Rolling load range daily.

        Args:
            date(datetime.datetime): date

        Returns:
            list: daily rolling load range list
        """
        return self._rolling_load_range_d.get(date, list())

    def rolling_load_ranges_minutely(self, date):
        """
        Rolling load ranges minutely.

        Args:
            date(datetime.datetime): date

        Returns:
            list: trading days list
        """
        return self._rolling_load_range_m.get(date, list())

    def trading_days(self, start_date=None, end_date=None, include_max_history=False, freq=None):
        """
        Get trading days.
        Args:
            start_date(datetime.datetime): start date
            end_date(datetime.datetime): end date
            include_max_history(boolean): whether to include max history
            freq(string): frequency

        Returns:
            list: trading days list
        """
        trading_days = self.trading_days_bt
        if include_max_history:
            if freq == 'm':
                trading_days = self.trading_days_minute_window + trading_days
            else:
                trading_days = self.trading_days_daily_window + trading_days
        if start_date is not None:
            trading_days = [trading_day for trading_day in trading_days if trading_day >= start_date]
        if end_date is not None:
            trading_days = [trading_day for trading_day in trading_days if trading_day <= end_date]
        return trading_days
