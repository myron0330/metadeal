# -*- coding: utf-8 -*-
# **********************************************************************************#
#     File: Portfolio file
# **********************************************************************************#


class StockPortfolio(object):
    """
    股票账户头寸，包含现金头寸和证券头寸两部分
    """
    def __init__(self, cash, positions=None):
        self.cash = cash
        self.positions = positions if positions else dict()
        self.avail_secpos = {sec: pos.amount for sec, pos in self.positions.iteritems()}
        self.set_avail_secpos()
        self.init_value_filled = False

    def fill_init_cost(self, init_prices):
        for sec, position in self.positions.iteritems():
            if position.cost is None:
                position.cost = init_prices[sec]
                position.value = init_prices[sec] * position.amount

    @classmethod
    def from_portfolio_info(cls, portfolio_info):
        """
        从 portfolio_info 初始化

        Args:
            portfolio_info(dict): 组合信息，包括 "cash" 和 "positions"

        Returns:
            StockPortfolio: 股票 portfolio 对象
        """
        cash, positions = portfolio_info['cash'], portfolio_info['positions']
        stock_portfolio = cls(cash)
        stock_portfolio.positions = positions
        stock_portfolio.avail_secpos = {security: position.available_amount
                                        for security, position in positions.iteritems()}
        return stock_portfolio

    @property
    def seccost(self):
        return {sec: pos.cost for sec, pos in self.positions.iteritems()}

    @property
    def secpos(self):
        return {sec: pos.amount for sec, pos in self.positions.iteritems()}

    @property
    def secpl(self):
        return {sec: pos.profit for sec, pos in self.positions.iteritems()}

    @property
    def secval(self):
        return {sec: pos.value for sec, pos in self.positions.iteritems()}

    @property
    def securities(self):
        """
        仓位中的所有证券列表
        """
        return [s for s, pos in self.positions.iteritems() if pos.amount > 0]

    def evaluate(self, data):
        """
        计算当前总的用户权益，现金 + 持仓权益

        Args:
            data (DataFrame): 行情数据

        Returns:
            float: 总的用户权益
        """

        v = self.cash
        for sec, pos in self.positions.iteritems():
            p = data.at[sec, 'closePrice']
            pos.value = p * pos.amount
            pos.profit = (p - pos.cost) * pos.amount
            v += p * pos.amount
        return v

    def set_avail_secpos(self):
        """
        设置可卖头寸
        """
        for symbol, position in self.positions.iteritems():
            self.avail_secpos[symbol] = position.amount
            position.available_amount = position.amount

    def show(self):
        """
        将相关信息显示为字典
        """

        output = {}
        for s, v in self.secpos.items():
            if v:
                output[s] = {'amount': v,
                             'cost': self.seccost.get(s, 0),
                             'value': self.secval.get(s, 0),
                             'P/L': self.secpl.get(s, 0)}
        return output

    def __repr__(self, indent=""):
        repr_str = u"[Portfolio]:\ncash = {}".format(self.cash)
        if self.secpos:
            repr_str += u"\nsecurity_position = {}\nsecurity_cost = {}".format(
                self.secpos, self.seccost)
        return repr_str.replace("\n", "\n"+indent)
