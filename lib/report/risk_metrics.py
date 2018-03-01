# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Risk metrics file
# **********************************************************************************#
import numpy as np
from datetime import datetime, timedelta
from ..data_loader.yieldcurve import load_yieldcurve_data
from ..utils.datetime_utils import get_end_date


def get_return(values):
    """
    根据价值计算收益率

    Args:
        values (list): 价值的列表

    Returns:
        list: 收益率列表
    """
    returns = [0]*(len(values)-1)
    for key, value in enumerate(values[1:]):
        if not values[key] or not value or np.isnan(value/values[key]):
            continue
        returns[key] = value/values[key] - 1.
    return returns


def get_cumulative_value(returns):
    """
    根据收益率计算累计价值

    Args:
        returns (list): 收益率列表

    Returns:
        list: 累计列表
    """

    values = returns[:]
    values[0] += 1
    for i, r in enumerate(values[1:]):
        values[i+1] = (1 + r) * values[i]
        if np.isnan(values[i+1]) or values[i+1] < 0:
            values[i+1] = 0
    return values


def get_annualized_return(c_values):
    """
    根据累计价值计算年化收益率

    Args:
        c_values (list): 累计价值列表

    Returns:
        float: 年化收益率
    """

    return c_values[-1] ** (250. / len(c_values)) - 1


def get_max_drawdown(c_values):
    """
    根据累计价值计算最大回撤

    Args:
        c_values (list): 累计价值列表

    Returns:
        float: 最大回撤
    """

    drawdown = [1 - v/max(1, max(c_values[:i+1])) for i, v in enumerate(c_values)]
    return max(drawdown)


def get_riskfree_rate(date):
    """
    获得无风险利率

    Args:
        date (datetime): 日期

    Returns:
        float: 从该日期往后第一个可用的无风险利率
    """

    return 0.035


def get_CAPM(st_returns, bm_returns, rf):
    """
    计算alpha和beta

    Args:
        st_returns (list): 策略收益率列表
        bm_returns (list): 市场收益率列表
        rf (float): 无风险收益率

    Returns:
        (float, float): alpha, beta
    """

    d = len(st_returns)

    if d == 1:
        return None, None

    mu_st = sum(st_returns) / d
    mu_bm = sum(bm_returns) / d
    mul = [st_returns[i] * bm_returns[i] for i in range(d)]

    cov = sum(mul) / d - mu_st * mu_bm
    var_bm = np.std(bm_returns) ** 2
    beta = cov / var_bm
    if beta in [-np.inf, np.inf] or np.isnan(beta):
        return None, None

    cummulative_st = get_cumulative_value(st_returns)
    annulized_st = get_annualized_return(cummulative_st)
    cummulative_bm = get_cumulative_value(bm_returns)
    annulized_bm = get_annualized_return(cummulative_bm)

    alpha = (annulized_st - rf) - beta * (annulized_bm - rf)
    return alpha, beta


def get_information_ratio(st_returns, bm_returns):
    """
    计算信息比率

    Args:
        st_returns (list): 策略收益率列表
        bm_returns (list): 市场收益率列表

    Returns:
        float: 信息比率
    """

    d = len(st_returns)

    if d == 1:
        return None

    diff = [st_returns[i] - bm_returns[i] for i in range(d)]
    IR = sum(diff) / d / np.std(diff, ddof=1) * 250**0.5
    if IR in [np.inf, -np.inf] or np.isnan(IR):
        return None
    return IR


def get_turnover_rate(buy_value, sell_value, portfolio_mean_value):
    """
    计算换手率

    Args:
        buy_value (float): 策略总买入价值
        sell_value (float): 策略总买入价值
        portfolio_mean_value (float): 策略平均持仓价值

    Returns:
        float: 换手率
    """

    TR = min(buy_value, sell_value) / portfolio_mean_value
    if TR in [np.inf, -np.inf] or np.isnan(TR):
        return None
    return TR

