# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: trading base.
# **********************************************************************************#
import ast
import codegen
from .. context.parameters import SimulationParameters
from .. context.strategy import TradingStrategy
from .. trade import Commission, Slippage
from .. const import PRESET_KEYARGS, ENFORCEMENT_COMPATIBLE_ACCOUNT


def get_code_parameters(strategy_code):
    """
    Get code parameters

    Args:
        strategy_code(string): strategy code
    """
    tree = ast.parse(strategy_code)
    root_node = tree.body
    keep_nodes = []
    for node in root_node:
        try:
            key = node.targets[0].id
            if key in ('accounts', 'refresh_rate', 'freq', 'capital_base', 'position_base', 'cost_base',
                       'initialize', 'handle_data', 'commission', 'slippage', 'margin_rate'):
                keep_nodes.append(node)
        finally:
            pass
    tree.body = keep_nodes
    return codegen.to_source(tree)


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
    start = parse_prior_params(config, local_variables, 'start', PRESET_KEYARGS['start'])
    end = parse_prior_params(config, local_variables, 'end', PRESET_KEYARGS['end'])
    max_history_window = \
        parse_prior_params(config, local_variables, 'max_history_window', PRESET_KEYARGS['max_history_window'])
    universe = parse_prior_params(config, local_variables, 'universe', PRESET_KEYARGS['universe'])
    benchmark = parse_prior_params(config, local_variables, 'benchmark', PRESET_KEYARGS['benchmark'])
    accounts = parse_prior_params(config, local_variables, 'accounts', PRESET_KEYARGS['accounts'])
    capital_base = parse_prior_params(config, local_variables, 'capital_base', PRESET_KEYARGS['capital_base'])
    position_base = parse_prior_params(config, local_variables, 'position_base', PRESET_KEYARGS['security_base'])
    cost_base = parse_prior_params(config, local_variables, 'cost_base', PRESET_KEYARGS['security_cost'])
    refresh_rate = parse_prior_params(config, local_variables, 'refresh_rate', PRESET_KEYARGS['refresh_rate'])
    freq = parse_prior_params(config, local_variables, 'freq', PRESET_KEYARGS['freq'])
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
        security_base=position_base,
        security_cost=cost_base,
        commission=commission,
        slippage=slippage,
        refresh_rate=refresh_rate,
        freq=freq,
        max_history_window=max_history_window,
        accounts=accounts,
        isbacktest=0,
        position_base_by_accounts=position_base_by_accounts,
        cost_base_by_accounts=cost_base_by_accounts,
        capital_base_by_accounts=capital_base_by_accounts,
        threaded=False,
    )
    return sim_params


def strategy_from_code(code, need_strategy_parse=True, log_obj=None):
    """
    执行策略代码文本，提取其中策略信息（sim_params, strategy, 新增用户自定义变量和引用包）
    Args:
        code: 策略代码
        need_strategy_parse(boolean): if strategy parse is needed
        log_obj(obj): log obj by user

    Returns:
        tuple: TradingStrategy, param dict
    """
    # 此处为了使得code能够通过execute正常运行，需要将策略可能调用的模块预先import
    from api import (Commission, Slippage, OrderState, AccountConfig)
    assert (Commission, Slippage, OrderState, AccountConfig)
    log = log_obj
    # 将当前环境中的local变量注入到globals中，用于执行code策略
    if not need_strategy_parse:
        code = get_code_parameters(code)
    exec code in locals()
    if need_strategy_parse:
        strategy = TradingStrategy(initialize=locals().get('initialize'), handle_data=locals().get('handle_data'))
    else:
        strategy = None
    return strategy, locals()


def orders_to_response(trading_orders, sub_portfolio_info, registered_accounts=None, key='account_type'):
    """

    Args:
        trading_orders(dict): trading orders by accounts
        sub_portfolio_info(dict): sub portfolio mapping info
        registered_accounts(dict): registered accounts info, account_name --> account obj
        key:

    Returns:

    """
    registered_accounts = registered_accounts or dict()
    if key == 'account_type':
        response = {'security': list(),
                    'futures': list(),
                    'index': list(),
                    'otc_fund': list()}
    elif key == 'account_name':
        response = {
            account_name: list() for account_name in registered_accounts
        }
    else:
        raise Exception('key is not valid.')
    for account_name, order_list in trading_orders.iteritems():
        if sub_portfolio_info:
            if ENFORCEMENT_COMPATIBLE_ACCOUNT in sub_portfolio_info:
                portfolio_id = sub_portfolio_info[ENFORCEMENT_COMPATIBLE_ACCOUNT]
            else:
                portfolio_id = sub_portfolio_info.get(account_name)
        else:
            portfolio_id = None
        adjusted_order_list = list()
        for order in order_list:
            item = {
                'symbol': order.symbol,
                'amount': order.order_amount,
                'order_type': order.order_type,
                'offset_flag': order.offset_flag,
                'direction': order.direction,
                'price': order.price,
                'portfolio_id': portfolio_id,
            }
            adjusted_order_list.append(item)
        if key == 'account_type':
            response[registered_accounts[account_name].account_type] += adjusted_order_list
        else:
            response[account_name] += adjusted_order_list
    return response


__all__ = [
    'get_code_parameters',
    'parse_prior_params',
    'parse_sim_params',
    'strategy_from_code',
    'orders_to_response'
]
