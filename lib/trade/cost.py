"""
# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: objects for calculating trading costs
#   Author: Myron
# **********************************************************************************#
"""


class Commission(object):
    """
    手续费标准，包含如下属性

    * self.buycost：买进手续费
    * self.sellcost：卖出手续费
    * self.unit：手续费单位
    """

    def __init__(self, buycost=0.001, sellcost=0.002, unit="perValue"):
        """
        初始化开仓、平仓的成本和单位

        Args:
            buycost (float): 开仓手续费
            sellcost (float): 平仓手续费
            unit (str): 手续费单位，可选值'perValue'或'perShare'

        Examples:
            >> commission = Commission()
            >> commission = Commission(0.0005, 0.0005, 'perShare')
        """
        self.buycost = float(buycost)
        self.sellcost = float(sellcost)
        if unit not in ['perValue', 'perShare']:
            raise ValueError('Exception in "Commission": unit must be perValue or perShare!')
        self.unit = unit

    def calculate_stock_commission(self, price, direction):
        """
        计算股票每股手续费
        Args:
            price (float): 成交价
            direction (int): 交易方向，1为买入，-1为卖出

        Returns:
            float: 每股手续费成本

        Examples:
            >> commission = Commission()
            >> commission.calculate_stock_commission(10.00, 1)
            >> commission.calculate_stock_commission(10.00, -1)
        """
        if direction > 0:
            if self.unit == 'perValue':
                cost_per_share = price * self.buycost
            else:
                cost_per_share = self.buycost
        else:
            if self.unit == 'perValue':
                cost_per_share = price * self.sellcost
            else:
                cost_per_share = self.sellcost
        return cost_per_share

    def calculate_futures_commission(self, market_value, offset_flag='open'):
        """
        计算期货手续费
        Args:
            market_value (float): 市值
            offset_flag (basestring): 开仓或平仓
        Returns:
            float: 手续费成本
        Examples:
            >> commission = Commission()
            >> commission.calculate_futures_commission(10000.00)
        """
        cost = self.buycost if offset_flag == 'open' else self.sellcost
        if self.unit == 'perValue':
            return cost * market_value
        else:
            return cost

    def calculate_otc_fund_commission(self, cash, order_type='purchase'):
        """
        计算场外基金手续费
        Args:
            cash (float): 总金额
            order_type (str): 下单类型, 'purchase' or 'redeem'
        Returns:
            float: 手续费成本
        Examples:
            >> commission = Commission()
            >> commission.calculate_futures_commission(10000.00)
        """
        if self.unit == 'perShare':
            raise Exception('The commission of OTC Fund account can not be set in  "perShare" mode! ')
        if order_type == 'purchase':
            return cash * self.buycost / (1. + self.buycost)
        else:
            return cash * self.sellcost

    def calculate_index_commission(self, market_value, offset_flag=None):
        """
        计算指数交易手续费
        Args:
            market_value (float): 总金额
            offset_flag (str): 下单类型, 'open' or 'close'分别对应开平
        Returns:
            float: 手续费成本
        Examples:
            >> commission = Commission()
            >> commission.calculate_index_commission(10000.00)
        """
        if self.unit == 'perShare':
            raise Exception('The commission of index account can not be set in  "perShare" mode! ')
        if offset_flag == 'open':
            return round(market_value * self.buycost, 2)
        else:
            return round(market_value * self.sellcost, 2)

    def __repr__(self):
        return "{class_name}(buycost={buycost}, sellcost = {sellcost}, " \
               "unit = {unit}".format(class_name=self.__class__.__name__, buycost=self.buycost,
                                      sellcost=self.sellcost, unit=self.unit)


class Slippage(object):

    def __init__(self, value=0, unit="perValue"):
        """
        初始化滑点的值和单位

        Args:
            value (float): 滑点值
            unit (str): 滑点单位，可选值'perValue'或'perShare'

        Examples:
            >> slippage = Slippage()
            >> slippage = Slippage(0.01, 'perShare')
        """

        self.value = float(value)
        if unit not in ['perValue', 'perShare']:
            raise ValueError('Exception in "Slippage": unit must be perValue or perShare!')
        self.unit = unit

    def calculate_stock_slippage(self, price):
        """
        计算股票滑点
        Args:
            price (float): 成交价

        Returns:
            float: 滑点成本

        Examples:
            >> slippage = Slippage()
            >> slippage.calculate_stock_slippage(10.00)
        """
        if self.unit == 'perValue':
            slippage_per_share = price * self.value
        else:
            slippage_per_share = self.value
        return slippage_per_share

    def calculate_futures_slippage(self, market_value):
        """
        计算期货滑点

        Args:
            market_value (float): 市值

        Returns:
            float: 滑点成本

        Examples:
            >> slippage = Slippage()
            >> slippage.calculate_futures_slippage(10000.00)
        """
        if self.unit == 'perValue':
            return self.value * market_value
        else:
            return self.value

    def calculate_index_slippage(self, market_value):
        """
        计算指数账户交易滑点

        Args:
            market_value (float): 市值

        Returns:
            float: 滑点成本

        Examples:
            >> slippage = Slippage()
            >> slippage.calculate_index_slippage(10000.00)
        """
        if self.unit == 'perValue':
            return self.value * market_value
        else:
            return self.value

    def __repr__(self):
        return "{class_name}(value = {value},unit = {unit})".format(
            class_name=self.__class__.__name__,
            value=self.value,
            unit=self.unit)
