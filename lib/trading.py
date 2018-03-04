# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
from . trading_engine import TradingEngine
from . account.account import AccountManager
from . context.clock import Clock
from . context.context import Context
from . context.parameters import SimulationParameters
from . context.strategy import TradingStrategy
from . data.data_portal import DataPortal
from . gateway.gateway import Gateway
from . market.market_engine import MarketEngine
from . const import DEFAULT_KEYWORDS


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
    from api import (Commission, Slippage, OrderState, OrderStateMessage, AccountConfig)
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
    data_portal.batch_load_data(sim_params, disable_service=['market_service'])
    account_manager = AccountManager.from_config(clock, sim_params, data_portal)
    context = Context(clock, sim_params, strategy,
                      market_service=data_portal.market_service,
                      universe_service=data_portal.universe_service,
                      asset_service=data_portal.asset_service,
                      calendar_service=data_portal.calendar_service,
                      account_manager=account_manager)
    trading_gateway = Gateway.with_event_engine()
    trading_agent = TradingEngine(clock, sim_params, strategy,
                                  data_portal, context, account_manager,
                                  market_engine=MarketEngine,
                                  trading_gateway=trading_gateway)
    trading_agent.initialize()
    trading_agent.start()
