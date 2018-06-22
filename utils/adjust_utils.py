# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: adjust algorithm File
# **********************************************************************************#
import numpy as np


def add_adj(time_bars, adjust_factor):
    """
    加法前复权

    Args:
        time_bars(list): 时间段区间
        adjust_factor(OrderedDict): 复权因子字典

    Returns:
        numpy.array: 复权因子序列
    """
    adj_array = np.array([adjust_factor.get(key, 0) for key in time_bars])
    added_factor = np.array(map(lambda x: sum(adj_array[x+1:]),
                                range(adj_array.size-1)) + [0])
    return added_factor


def mul_adj(time_bars, adjust_factor):
    """
    乘法前复权

    Args:
        time_bars(list): 时间段区间
        adjust_factor(OrderedDict): 复权因子字典

    Returns:
        numpy.array: 复权因子序列
    """
    adj_array = np.array([adjust_factor.get(key, 1) for key in time_bars])
    multiplied_factor = np.array(map(lambda x: reduce(lambda a, b: a * b, adj_array[x+1:]),
                                     range(adj_array.size-1)) + [1])
    return multiplied_factor


def adj_operator(data, adjust_matrix, adj):
    """
    复权操作符

    Args:
        data(numpy.array): 行情矩阵
        adjust_matrix(numpy.array): 复权矩阵
        adj(string): 复权类型

    Returns:
        numpy.array: 复权因子序列
    """
    matrix = data
    if adj == 'add':
        matrix = data + adjust_matrix
    if adj == 'mul':
        matrix = data * adjust_matrix
    return matrix.astype(np.double).round(2)


def adj_func_choose(adj):
    """
    选取复权函数

    Args:
        adj(string): 复权类型

    Returns:
        func: 复权函数
    """
    if adj == 'add':
        return add_adj
    if adj == 'mul':
        return mul_adj
    return None


def adj_matrix_choose(adj, shape):
    """
    选取复权矩阵

    Args:
        adj(string): 复权类型
        shape(tuple): 矩阵

    Returns:
        func: 复权函数
    """
    if adj == 'add':
        return np.zeros(shape)
    if adj == 'mul':
        return np.ones(shape)
    return None


__all__ = [
    'add_adj',
    'adj_func_choose',
    'adj_matrix_choose',
    'adj_operator',
    'mul_adj'
]
