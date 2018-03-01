# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Performance parse file
# **********************************************************************************#


def perf_parse(backtest_result, perf):
    """
    策略表现解析的包装函数，主要用于兼容quartz 1.x，输入为backtest的输出

    Args:
        backtest_result (object): 任意内容
        perf (dict): 策略表现

    Returns:
        dict: 策略表现，键为风险指标名称，值为风险指标对应的结果

    Examples:
        >> bt, perf, bt_by_account = backtest(...)
        >> perf = perf_parse(bt, perf)
    """

    return perf
