# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
import json
from . account.account import AccountManager
from . const import DEFAULT_KEYWORDS
from . context.context import Context
from . context.parameters import SimulationParameters
from . context.strategy import TradingStrategy
from . core.clock import Clock
from . data.data_portal import DataPortal
from . event.event_engine import EventEngine
from . gateway import (
    PMSGateway,
    StrategyGateway,
    SubscriberGateway,
    CTPMarketGateway,
    CTPGateway
)
from . trading_engine import TradingEngine
from . trading_base import strategy_from_code, parse_sim_params
from . configs import ctp_config


def trading(strategy_code, config=None, connect_json=None, **kwargs):
    """
    Trading function according to strategy code.

    Args:
        strategy_code(string): strategy code
        config(dict): config parameters
        connect_json(string): connection json path
        **kwargs: key-value parameters
    """
    with open(connect_json, 'r+') as connect_file:
        connect_json = json.load(connect_file)
    print connect_json
    config = config or dict()
    strategy, local_variables = strategy_from_code(strategy_code)
    sim_params = parse_sim_params(config, local_variables)
    clock = Clock(sim_params.freq)
    data_portal = DataPortal()
    data_portal.batch_load_data(sim_params, disable_service=['market_service'])
    event_engine = EventEngine()
    strategy_gateway = StrategyGateway()
    ctp_gateway = CTPGateway.from_config(ctp_config, event_engine=event_engine)
    # subscriber_gateway = SubscriberGateway.from_config(sim_params=sim_params, event_engine=event_engine)
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
    trading_engine = TradingEngine(clock, sim_params, strategy,
                                   data_portal, context, account_manager,
                                   event_engine=event_engine,
                                   subscriber_gateway=subscriber_gateway,
                                   strategy_gateway=strategy_gateway,
                                   pms_gateway=pms_gateway)
    trading_engine.initialize()
    trading_engine.start()
