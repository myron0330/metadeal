# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
import bisect
import datetime
import time
from lib.trading import trading_orders_minutely, trading_orders_daily
from lib.trading_base import *
from lib.event import EventEngine, EventType
from lib.backtest import backtest
from lib.instrument.data_portal import DataPortal
from lib.context.parameters import SimulationParameters
from lib.market.market_roller import MarketRoller
from utils.datetime_utils import (
    normalize_date,
    get_trading_days,
    get_direct_trading_day_list)
from lib.const import DEFAULT_KEYWORDS


class BarSimulator(object):

    def __init__(self,
                 start=None,
                 end=None,
                 date=None,
                 universe=None,
                 freq='d'):
        self.start, self.end = self._init_dates(start=start, end=end, date=date)
        self.universe = universe
        self.freq = freq
        self.sim_params = self._init_sim_params()
        self.data_portal = self._init_data_portal()
        self.market_roller = MarketRoller(
            universe=universe,
            market_service=self.data_portal.market_service,
            trading_days=self.data_portal.calendar_service.trading_days,
            daily_bar_loading_rate=1,
            minute_bar_loading_rate=1
        )

    @staticmethod
    def _init_dates(start=None, end=None, date=None):
        """
        Start dates.
        """
        if start and end:
            trading_days = get_trading_days(start, end)
            start, end = trading_days[0], trading_days[-1]
        if date:
            start = end = normalize_date(date)
        return start, end

    def _init_sim_params(self):
        """
        Init sim parameters.
        """
        sim_params = SimulationParameters(
            start=self.start,
            end=self.end,
            universe=self.universe,
            benchmark=self.universe[0],
            freq=self.freq,
            max_history_window=(1, 241)
        )
        return sim_params

    def _init_data_portal(self):
        """
        Init data portal.
        """
        data_portal = DataPortal()
        data_portal.batch_load_data(self.sim_params)
        if self.freq == 'm':
            trading_days = data_portal.calendar_service.trading_days
            data_portal.market_service.rolling_load_minute_data(trading_days=trading_days)
        return data_portal

    def publish_minute_bars(self, date=None):
        """
        publish minute bars by date.

        Args:
            date(string or datetime.datetime): input date
        """
        current_date = normalize_date(date or self.end)
        self.market_roller.prepare_minute_data(current_date=current_date)
        minute_bars = self.market_roller.tas_minute_expanded_cache.get(current_date, dict())
        sorted_minutes = sorted(minute_bars)
        minute_length = len(minute_bars)
        night_start = min(bisect.bisect_right(sorted_minutes, '16:00'), minute_length)
        sorted_minutes = sorted_minutes[night_start:] + sorted_minutes[:night_start]
        for bar_minute in sorted_minutes:
            bar_data = minute_bars.get(bar_minute)
            if not bar_data:
                continue
            bar_response = {
                symbol: list(value[:5]) + [value[4]] for symbol, value in bar_data.iteritems()
            }
            yield [(bar_minute, bar_response)]


# todo: 暂且不传入minute，从分钟线一开始滚动一整天
def paper_strategy_minutely(date, context_path, portfolio_mapping, strategy_code, previous_code, log_obj=None):
    capital_base_by_accounts = {
        'fantasy_account': 999999999,
    }
    position_base_by_accounts = {'fantasy_account': {}}
    cost_base_by_accounts = {'fantasy_account': {}}
    config = {
        'start': date,
        'end': date,
        'capital_base_by_accounts': capital_base_by_accounts,
        'position_base_by_accounts': position_base_by_accounts,
        'cost_base_by_accounts': cost_base_by_accounts
    }
    strategy, local_variables = strategy_from_code(previous_code, log_obj=log_obj)
    params = parse_sim_params(config, local_variables)
    initialize = local_variables.get('initialize', PRESET_KEYARGS['initialize'])
    handle_data = local_variables.get('handle_data')
    post_trading_day = local_variables.get('post_trading_day', PRESET_KEYARGS['post_trading_day'])
    # back test strategy_code 1 month before date
    trading_days = get_direct_trading_day_list(date, step=10, forward=False)
    start = trading_days[0]
    end = trading_days[-2]
    bt, perf, bt_by_account = backtest(universe=params.universe, start=start, end=end,
                                       initialize=initialize, handle_data=handle_data,
                                       post_trading_day=post_trading_day, refresh_rate=params.refresh_rate,
                                       freq=params.freq, accounts=params.accounts)
    # get position_base
    capital_base_by_accounts['fantasy_account'] = bt.cash.iat[-1]
    positions = bt.security_position.iat[-1]
    position_base = {k: v['amount'] for k, v in positions.iteritems()}
    cost_base = {k: v['cost'] for k, v in positions.iteritems()}
    position_base_by_accounts = {'fantasy_account': position_base}
    cost_base_by_accounts = {'fantasy_account': cost_base}
    config = {
        'start': params.trading_days[-1],
        'end': params.trading_days[-1],
        'capital_base_by_accounts': capital_base_by_accounts,
        'position_base_by_accounts': position_base_by_accounts,
        'cost_base_by_accounts': cost_base_by_accounts
    }
    event_engine = EventEngine()
    # event_engine.register_handlers(EventType.event_order_response, service_order_to_pms)
    event_engine.start()
    trading_agent = trading_orders_minutely(strategy_code=strategy_code, config=config, event_engine=event_engine,
                                            pms_host='', context_path=context_path,
                                            sub_portfolio_info=portfolio_mapping, debug=True, log_obj=log_obj)
    event_engine.publish(EventType.event_start)

    test_universe = trading_agent.data_portal.universe_service.full_universe
    bar_simulator = BarSimulator(date=date, universe=test_universe, freq='m')

    for data in bar_simulator.publish_minute_bars():
        parameters = {
            'bar': data
        }
        event_engine.publish(EventType.event_on_bar, **parameters)
        time.sleep(2)
    event_engine.publish(EventType.event_stop)
    event_engine.stop()


def paper_strategy_daily(date, context_path, portfolio_mapping, strategy_code, previous_code, log_obj=None):
    capital_base_by_accounts = {
        'fantasy_account': 99999999,
    }
    position_base_by_accounts = {'fantasy_account': {}}
    cost_base_by_accounts = {'fantasy_account': {}}
    config = {
        'start': date,
        'end': date,
        'capital_base_by_accounts': capital_base_by_accounts,
        'position_base_by_accounts': position_base_by_accounts,
        'cost_base_by_accounts': cost_base_by_accounts
    }
    strategy, local_variables = strategy_from_code(previous_code, log_obj=log_obj)
    params = parse_sim_params(config, local_variables)
    initialize = local_variables.get('initialize', PRESET_KEYARGS['initialize'])
    handle_data = local_variables.get('handle_data')
    post_trading_day = local_variables.get('post_trading_day', PRESET_KEYARGS['post_trading_day'])
    # back test strategy_code 1 month before date
    trading_days = get_direct_trading_day_list(date, step=30, forward=False)
    start = trading_days[0]
    end = trading_days[-2]
    bt, perf, bt_by_account = backtest(universe=params.universe, start=start, end=end,
                                       initialize=initialize, handle_data=handle_data,
                                       post_trading_day=post_trading_day, refresh_rate=params.refresh_rate,
                                       freq=params.freq, accounts=params.accounts)
    # get position_base
    capital_base_by_accounts['fantasy_account'] = bt.cash.iat[-1]
    positions = bt.security_position.iat[-1]
    position_base = {k: v['amount'] for k, v in positions.iteritems()}
    cost_base = {k: v['cost'] for k, v in positions.iteritems()}
    position_base_by_accounts = {'fantasy_account': position_base}
    cost_base_by_accounts = {'fantasy_account': cost_base}
    config = {
        'start': params.trading_days[-1],
        'end': params.trading_days[-1],
        'capital_base_by_accounts': capital_base_by_accounts,
        'position_base_by_accounts': position_base_by_accounts,
        'cost_base_by_accounts': cost_base_by_accounts
    }
    orders = trading_orders_daily(strategy_code, config, portfolio_mapping,
                                  context_path=context_path, pms_host='', log_obj=log_obj)
    # service_order_to_pms(orders)
    return True


if __name__ == '__main__':
    test_universe = ['000001.XSHE', '600000.XSHG', '000300.ZICN']
    bar_simulator = BarSimulator(date='20180507', universe=test_universe, freq='m')
    for data in bar_simulator.publish_minute_bars():
        print data
