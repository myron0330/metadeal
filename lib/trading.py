# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
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
    SubscriberGateway
)
from . trading_engine import TradingEngine


def _parse_prior_params(bt_config, code_config, default_config, key, prior='pre'):
    """
    Parse parameters with priority.

    Args:
        bt_config(dict): bt config
        code_config(dict): code config
        default_config(default): default value
        key(string): parameter name
        prior(string): use bt_config in prior if 'pre' otherwise code_config

    Returns:
        value
    """
    if prior == 'pre':
        return bt_config.get(key, code_config.get(key, default_config.get(key)))
    else:
        return code_config.get(key, bt_config.get(key, default_config.get(key)))


def _parse_sim_params(config, local_variables):
    """
    Parse simulation parameters

    Args:
        config(dict): config parameters
        local_variables(dict): parser parameters
    """
    start = _parse_prior_params(config, local_variables, DEFAULT_KEYWORDS, 'start')
    end = _parse_prior_params(config, local_variables, DEFAULT_KEYWORDS, 'end')
    benchmark = _parse_prior_params(config, local_variables, DEFAULT_KEYWORDS, 'benchmark')
    universe = _parse_prior_params(config, local_variables, DEFAULT_KEYWORDS, 'universe')
    capital_base = _parse_prior_params(config, local_variables, DEFAULT_KEYWORDS, 'capital_base')
    position_base = _parse_prior_params(config, local_variables, DEFAULT_KEYWORDS, 'position_base')
    cost_base = _parse_prior_params(config, local_variables, DEFAULT_KEYWORDS, 'cost_base')
    commission = _parse_prior_params(config, local_variables, DEFAULT_KEYWORDS, 'commission')
    slippage = _parse_prior_params(config, local_variables, DEFAULT_KEYWORDS, 'slippage')
    refresh_rate = _parse_prior_params(config, local_variables, DEFAULT_KEYWORDS, 'refresh_rate')
    freq = _parse_prior_params(config, local_variables, DEFAULT_KEYWORDS, 'freq')
    max_history_window = _parse_prior_params(config, local_variables, DEFAULT_KEYWORDS, 'max_history_window')
    accounts = _parse_prior_params(config, local_variables, DEFAULT_KEYWORDS, 'accounts')
    sim_params = SimulationParameters(
        start=start,
        end=end,
        benchmark=benchmark,
        universe=universe,
        capital_base=capital_base,
        position_base=position_base,
        cost_base=cost_base,
        commission=commission,
        slippage=slippage,
        refresh_rate=refresh_rate,
        freq=freq,
        max_history_window=max_history_window,
        accounts=accounts
    )
    return sim_params


def _strategy_from_code(strategy_code):
    """
    Execute strategy code and extract user defined variables

    Args:
        strategy_code(string): strategy code

    Returns:
        tuple: TradingStrategy, param dict
    """
    # 此处为了使得code能够通过execute正常运行，需要将策略可能调用的模块预先import
    # 将当前环境中的local变量注入到globals中，用于执行code策略
    from api import (Commission, Slippage, OrderState, AccountConfig)
    assert (Commission, Slippage, OrderState, AccountConfig)
    exec strategy_code in locals()
    strategy = TradingStrategy(**locals())
    return strategy, locals()


def trading(strategy_code, config=None, **kwargs):
    """
    Trading function according to strategy code.

    Args:
        strategy_code(string): strategy code
        config(dict): config parameters
        **kwargs: key-value parameters
    """
    config = config or dict()
    strategy, local_variables = _strategy_from_code(strategy_code)
    sim_params = _parse_sim_params(config, local_variables)
    clock = Clock(sim_params.freq)
    data_portal = DataPortal()
    data_portal.batch_load_data(sim_params)
    event_engine = EventEngine()
    strategy_gateway = StrategyGateway()
    subscriber_gateway = SubscriberGateway.from_config(sim_params=sim_params, event_engine=event_engine)
    pms_gateway = PMSGateway.from_config(clock, sim_params, data_portal,
                                         subscriber_gateway=subscriber_gateway)
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
