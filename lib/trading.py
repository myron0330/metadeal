# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
import json
from . core.clock import Clock
from . context.context import Context
from . account.account import AccountManager
from . data.data_portal import DataPortal
from . event.event_engine import EventEngine
from . trading_schedular import TradingScheduler
from . gateway import (
    PMSGateway,
    CTPGateway
)
from . market import MarketRoller
from . trading_base import (
    strategy_from_code,
    parse_sim_params
)
from . trading_agent import TradingAgent
from . configs import ctp_config


def trading(strategy_code, config=None, connect_json=None, debug=False, **kwargs):
    """
    Trading function according to strategy code.

    Args:
        strategy_code(string): strategy code
        config(dict): config parameters
        connect_json(string): connection json path
        debug(boolean): whether to debug
        **kwargs: key-value parameters
    """
    # with open(connect_json, 'r+') as connect_file:
    #     connect_json = json.load(connect_file)
    # print connect_json
    config = config or dict()
    strategy, local_variables = strategy_from_code(strategy_code)
    sim_params = parse_sim_params(config, local_variables)
    clock = Clock(sim_params.freq)
    data_portal = DataPortal()
    data_portal.batch_load_data(sim_params)
    event_engine = EventEngine()
    trading_scheduler = TradingScheduler(start=sim_params.start, end=sim_params.end,
                                         freq=sim_params.freq,
                                         refresh_rate_d=sim_params.refresh_rate,
                                         refresh_rate_m=sim_params.refresh_rate,
                                         max_history_window_d=sim_params.max_history_window_daily,
                                         max_history_window_m=sim_params.max_history_window_minute,
                                         calendar_service=data_portal.calendar_service)
    market_roller = MarketRoller(
            universe=list(set(data_portal.universe_service.full_universe)),
            market_service=data_portal.market_service,
            trading_days=data_portal.calendar_service.all_trading_days,
            daily_bar_loading_rate=60,
            minute_bar_loading_rate=5,
            debug=debug,
            paper=True)
    ctp_gateway = CTPGateway.from_config(ctp_config, event_engine=event_engine)
    pms_gateway = PMSGateway.from_config(clock, sim_params, data_portal,
                                         ctp_gateway=ctp_gateway)
    account_manager = AccountManager.from_config(clock, sim_params, data_portal,
                                                 event_engine=event_engine,
                                                 pms_gateway=pms_gateway)
    context = Context(clock, sim_params, strategy,
                      market_service=data_portal.market_service,
                      universe_service=data_portal.universe_service,
                      asset_service=data_portal.asset_service,
                      calendar_service=data_portal.calendar_service,
                      account_manager=account_manager)
    trading_agent = TradingAgent(clock=clock,
                                 sim_params=sim_params,
                                 strategy=strategy,
                                 data_portal=data_portal,
                                 context=context,
                                 account_manager=account_manager,
                                 market_roller=market_roller,
                                 trading_scheduler=trading_scheduler,
                                 event_engine=event_engine)
    trading_agent.register_handlers(event_engine=event_engine)
    trading_agent.prepare_initialize(minute_loading_rate=5)
    trading_agent.pre_trading_day(clock.current_date)
    trading_agent.rolling_load_minute_data(trading_scheduler.rolling_load_ranges_minutely(clock.current_date))
    trading_agent.pre_trading_minute(clock.current_date)
    trading_agent.start()
