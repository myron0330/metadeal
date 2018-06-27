# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: trading base.
# **********************************************************************************#
from utils.datetime_utils import get_clearing_date_of
from . context.parameters import SimulationParameters
from . context.strategy import TradingStrategy
from . trade import Commission, Slippage
from . const import DEFAULT_KEYWORDS


def parse_prior_params(bt_config, code_config, key, default, prior='pre'):
    """
    Parse parameters with priority.

    Args:
        bt_config(dict): bt config
        code_config(dict): code config
        key(string): parameter name
        default(default): default value
        prior(string): use bt_config in prior if 'pre' otherwise code_config

    Returns:
        value
    """
    if prior == 'pre':
        return bt_config.get(key, code_config.get(key, default))
    else:
        return code_config.get(key, bt_config.get(key, default))


def parse_sim_params(config, local_variables):
    """
    Prepare simulation parameters.

    Args:
        config(dict): user config
        local_variables(dict): local variables from exec code

    Returns:
        SimulationParameters: simulation parameters.
    """
    start = parse_prior_params(config, local_variables, 'start', get_clearing_date_of())
    end = parse_prior_params(config, local_variables, 'end', get_clearing_date_of())
    max_history_window = \
        parse_prior_params(config, local_variables, 'max_history_window', DEFAULT_KEYWORDS['max_history_window'])
    universe = parse_prior_params(config, local_variables, 'universe', DEFAULT_KEYWORDS['universe'])
    benchmark = parse_prior_params(config, local_variables, 'benchmark', DEFAULT_KEYWORDS['benchmark'])
    accounts = parse_prior_params(config, local_variables, 'accounts', DEFAULT_KEYWORDS['accounts'])
    capital_base = parse_prior_params(config, local_variables, 'capital_base', DEFAULT_KEYWORDS['capital_base'])
    position_base = parse_prior_params(config, local_variables, 'position_base', DEFAULT_KEYWORDS['position_base'])
    cost_base = parse_prior_params(config, local_variables, 'cost_base', DEFAULT_KEYWORDS['cost_base'])
    refresh_rate = parse_prior_params(config, local_variables, 'refresh_rate', DEFAULT_KEYWORDS['refresh_rate'])
    freq = parse_prior_params(config, local_variables, 'freq', DEFAULT_KEYWORDS['freq'])
    commission = parse_prior_params(config, local_variables, 'commission', Commission())
    slippage = parse_prior_params(config, local_variables, 'slippage', Slippage())
    position_base_by_accounts = parse_prior_params(config, local_variables, 'position_base_by_accounts', dict())
    cost_base_by_accounts = parse_prior_params(config, local_variables, 'cost_base_by_accounts', dict())
    capital_base_by_accounts = parse_prior_params(config, local_variables, 'capital_base_by_accounts', dict())
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
        accounts=accounts,
        position_base_by_accounts=position_base_by_accounts,
        cost_base_by_accounts=cost_base_by_accounts,
        capital_base_by_accounts=capital_base_by_accounts,
    )
    return sim_params


def strategy_from_code(code, log_obj=None):
    """
    执行策略代码文本，提取其中策略信息（sim_params, strategy, 新增用户自定义变量和引用包）
    Args:
        code: 策略代码
        log_obj(obj): log obj by user

    Returns:
        tuple: TradingStrategy, param dict
    """
    from account.account import AccountConfig
    from trade import Commission, Slippage, OrderState
    assert (Commission, Slippage, OrderState, AccountConfig)
    log = log_obj
    exec code in locals()
    strategy = TradingStrategy(initialize=locals().get('initialize'), handle_data=locals().get('handle_data'))
    return strategy, locals()


def orders_to_response(trading_orders, sub_portfolio_info, registered_accounts=None, key='account_type'):
    """

    Args:
        trading_orders(dict): trading orders by accounts
        sub_portfolio_info(dict): sub portfolio mapping info
        registered_accounts(dict): registered accounts info, account_name --> account obj
    """
    pass


__all__ = [
    'parse_prior_params',
    'parse_sim_params',
    'strategy_from_code',
    'orders_to_response'
]
