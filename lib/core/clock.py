# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Clock file
# **********************************************************************************#
from datetime import datetime
from utils.datetime_utils import (
    get_previous_trading_date, get_current_date,
    get_current_minute, get_previous_minute,
    get_next_trading_date, get_previous_date, get_next_date,
    get_upcoming_trading_date, get_latest_trading_date,
    get_trading_days
)
from utils.decorator_utils import singleton


@singleton
class Clock(object):
    """
    Clock in globals
    """
    def __init__(self, freq='d'):
        self.freq = freq
        self._current_date = None
        self._previous_date = None
        self._next_date = None
        self._previous_trading_date = None
        self._next_trading_date = None
        self._upcoming_trading_date = None
        self._latest_trading_date = None
        self._current_minute = None
        self._previous_minute = None
        self._is_trading_day = True

    @property
    def current_date(self):
        """
        Current date
        """
        self.update_time_with_(date=True)
        return self._current_date

    @property
    def previous_date(self):
        """
        Previous date
        """
        self.update_time_with_(date=True)
        return self._previous_date

    @property
    def next_date(self):
        """
        Next date
        """
        self.update_time_with_(date=True)
        return self._next_date

    @property
    def previous_trading_date(self):
        """
        Previous trading date
        """
        self.update_time_with_(date=True)
        return self._previous_trading_date

    @property
    def next_trading_date(self):
        """
        Next trading date | benchmarking trading day
        """
        self.update_time_with_(date=True)
        return self._next_trading_date

    @property
    def upcoming_trading_date(self):
        """
        Upcoming trading date | benchmarking natural day
        """
        self.update_time_with_(date=True)
        return self._upcoming_trading_date

    @property
    def clearing_date(self):
        """
        Clearing date
        """
        if self.current_minute <= '20:00' and self.is_trading_day:
            return self.current_date
        return self.upcoming_trading_date

    @property
    def previous_clearing_date(self):
        """
        Previous clearing date.
        """
        if self.current_minute <= '20:00' and self.is_trading_day:
            return self.previous_date
        return self.latest_trading_date

    @property
    def latest_trading_date(self):
        """
        Latest trading date
        """
        self.update_time_with_(date=True)
        return self._latest_trading_date

    @property
    def current_minute(self):
        """
        Current minute
        """
        self.update_time_with_(minute=True)
        return self._current_minute

    @property
    def second(self):
        return datetime.now().second

    @property
    def previous_minute(self):
        """
        Previous minute
        """
        self.update_time_with_(minute=True)
        return self._previous_minute

    @property
    def is_trading_day(self):
        """
        whether or not trading day today
        """
        self.update_time_with_(date=True)
        return self._is_trading_day

    def update_time_with_(self, date=False, minute=False, date_type='str'):
        """
        Update time with parameters

        Args:
            date(boolean): whether to update date information
            minute(boolean): whether to update minute information
            date_type(string): the return type of date
        """
        if date:
            current_date = get_current_date()
            if self._current_date != current_date:
                self._current_date = current_date
                self._previous_date = get_previous_date()
                self._next_date = get_next_date()
                self._previous_trading_date = get_previous_trading_date()
                self._next_trading_date = get_next_trading_date()
                trading_day = get_trading_days(start=current_date, end=current_date)
                self._is_trading_day = bool(trading_day)
                self._upcoming_trading_date = get_upcoming_trading_date(is_trading_day=self._is_trading_day)
                self._latest_trading_date = get_latest_trading_date(date=self._current_date)
        if minute:
            current_minute = get_current_minute()
            if self._current_minute != current_minute:
                self._current_minute = current_minute
                self._previous_minute = get_previous_minute()

    @property
    def now(self):
        """
        Returns:
             datetime: current timestamp
        """
        if self.current_minute:
            hour = int(self.current_minute.split(':')[0])
            minute = int(self.current_minute.split(':')[1])
        else:
            hour, minute = 0, 0
        second = 0
        return self.with_(hour=hour, minute=minute, second=second)

    def with_(self, hour=None, minute=None, second=None, previous_date=False):
        """
        Format specific timestamp

        Args:
            hour (int): hours
            minute (int): minutes
            second (int): seconds
            previous_date (boolean): whether to use previous date or not

        Returns:
            datetime: the specific timestamp
        """
        date = self.current_date if not previous_date else self.previous_trading_date
        return datetime(date.year, date.month, date.day, hour or date.hour,
                        minute or date.minute, second or self.current_date.second)

    def __repr__(self):
        """
        Returns:
             datetime: current timestamp
        """
        return self.now.strftime('%Y-%m-%d %H:%M')


clock = Clock()
