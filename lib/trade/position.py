# -*- coding: utf-8 -*-
from __future__ import division
from . dividend import Dividend
from .. utils.error_utils import Errors


class Position(object):

    """
    股票持仓明细
    """
    __slots__ = ['symbol', 'amount', 'cost', 'profit',
                 'value', 'available_amount', 'dividends']

    def __init__(self, symbol, amount, cost, profit=0.0, value=None, available_amount=0):
        self.symbol = symbol
        self.amount = amount
        self.cost = cost
        self.profit = profit
        self.value = value
        self.available_amount = available_amount
        self.dividends = None

    def evaluate(self, price):
        self.value = price * self.amount
        self.profit = (price - self.cost) * self.amount

    @staticmethod
    def from_dict(position_dict, position_cost_dict):
        return {symbol: Position(symbol, pos, position_cost_dict.get(symbol), 0.0, pos) for symbol, pos
                in position_dict.iteritems()}

    def to_dict(self):
        """
        To dict
        """
        return {
            'symbol': self.symbol,
            'amount': self.amount,
            'cost': self.cost,
            'profit': self.profit,
            'value': self.value,
            'available_amount': self.available_amount,
            'dividends': self.dividends
        }

    def __getitem__(self, key, default=None):
        item_value = getattr(self, key) if getattr(self, key) else default
        return item_value

    def __repr__(self):
        return "Position(symbol: {}, amount: {}, available_amount: {}, cost: {}, profit: {}, " \
               "value: {})".format(self.symbol, self.amount, self.available_amount,
                                   self.cost, self.profit, self.value)


class PMSPosition(Position):
    """
    PMS position
    """

    def __init__(self, symbol, amount, cost, profit=0.0, value=None, available_amount=0,
                 dividends=None, total_cost=None):
        super(PMSPosition, self).__init__(symbol, amount, cost,
                                          profit=profit, value=value,
                                          available_amount=available_amount)
        self.total_cost = total_cost or self.cost * self.amount
        self.dividends = dividends

    @classmethod
    def from_request(cls, request):
        """
        Generate new position from request

        Args:
            request(dict): request data
        """
        return cls(**request)

    @classmethod
    def from_query(cls, query_data):
        """
        Recover existed order from query data

        Args:
            query_data(dict): query data
        """
        position = cls(**query_data)
        dividends = query_data.get('dividends')
        position.dividends = Dividend(**dividends) if dividends else None
        return position

    def to_database_item(self):
        """
        To redis item
        """
        redis_item = {
            'symbol': self.symbol,
            'amount': self.amount,
            'cost': self.cost,
            'total_cost': self.total_cost,
            'profit': self.profit,
            'value': self.value,
            'available_amount': self.available_amount,
            'dividends': None
        }
        if self.dividends:
            redis_item['dividends'] = self.dividends.to_dict()
        return redis_item


class LongShortPosition(object):
    """
    期货持仓类型
    """
    __slots__ = ['symbol', 'price', 'long_amount', 'long_cost', 'short_amount', 'short_cost',
                 'long_margin', 'short_margin', 'value', 'profit']

    def __init__(self, symbol, price=None, long_amount=0, short_amount=0, long_margin=0, short_margin=0,
                 long_cost=0, short_cost=0, value=0, profit=0):
        self.symbol = symbol
        self.price = price
        self.long_amount = long_amount
        self.short_amount = short_amount
        self.long_margin = long_margin
        self.short_margin = short_margin
        self.long_cost = long_cost
        self.short_cost = short_cost
        self.value = value
        self.profit = profit

    def evaluate(self, reference_price, multiplier=1., margin_rate=1.):
        """
        更新价格、margin及profit，返回增量的逐笔浮盈。默认进行合约乘数为1, 保证金率为1的估值

        Args:
            reference_price(float): 价格
            multiplier(float): 合约乘数
            margin_rate(float): 保证金率

        Returns(float): 价格变化产生的浮动盈亏增量

        """
        long_mv = reference_price * self.long_amount * multiplier
        short_mv = reference_price * self.short_amount * multiplier
        if not self.value:
            self.value = multiplier * (self.long_cost * self.long_amount - self.short_cost * self.short_amount)
        float_pnl_added = long_mv - short_mv - self.value
        self.price = reference_price
        self.long_margin = long_mv * margin_rate
        self.short_margin = short_mv * margin_rate
        self.profit = long_mv - short_mv - multiplier * (
            self.long_cost * self.long_amount - self.short_cost * self.short_amount)
        self.value = long_mv - short_mv
        return self, float_pnl_added

    @staticmethod
    def from_dict(position_dict, position_cost_dict):
        return {symbol: Position(symbol, pos, position_cost_dict.get(symbol), 0.0, pos) for symbol, pos
                in position_dict.iteritems()}

    def to_dict(self):
        """
        To dict
        """
        return {
            'symbol': self.symbol,
            'price': self.price,
            'long_amount': self.long_amount,
            'long_cost': self.long_cost,
            'long_margin': self.long_margin,
            'short_amount': self.short_amount,
            'short_cost': self.short_cost,
            'short_margin': self.short_margin,
            'value': self.value,
            'profit': self.profit,
        }

    def get(self, key, default=None):
        return self.__dict__.get(key, default)

    def __repr__(self):
        return "Position(symbol: {}, price: {}, long_amount: {}, short_amount: {}, long_margin: {}, short_margin: {}," \
               " long_cost: {}, short_cost: {}, profit: {})".format(self.symbol, self.price, self.long_amount,
                                                                    self.short_amount, self.long_margin,
                                                                    self.short_margin, self.long_cost,
                                                                    self.short_cost, self.profit)


class FuturesPosition(LongShortPosition):

    def __init__(self, symbol, price, long_amount=0, short_amount=0, long_margin=0, short_margin=0,
                 long_cost=0, short_cost=0, value=0, profit=0, today_long_open=0, today_short_open=0, today_profit=0):
        super(FuturesPosition, self).__init__(symbol, price, long_amount, short_amount, long_margin, short_margin,
                                              long_cost, short_cost, value, profit)
        self.today_long_open = today_long_open
        self.today_short_open = today_short_open
        self.today_profit = today_profit
        # self.total_long_cost = total_long_cost #or self.long_cost * self.long_amount * self.multiplier
        # self.total_short_cost = total_short_cost #or self.short_cost * self.short_amount * self.multiplier

    def calc_close_pnl(self, trade, multiplier):
        """
        仅计算并返回平仓盈亏，不更新价格、amount及value

        Args:
            trade(PMSTrade): 成交记录
            multiplier(float): 合约乘数

        Returns(float): 平仓盈亏

        """
        amount = self.long_amount if trade.direction == 1 else self.short_amount
        if amount < trade.filled_amount:
            raise Errors.INVALID_FILLED_AMOUNT
        cost = self.long_cost if trade.direction == 1 else self.short_cost
        close_pnl = trade.direction * (trade.transact_price - cost) * trade.filled_amount * multiplier
        return close_pnl

    @classmethod
    def from_request(cls, request):
        """
        Generate new FuturesPosition from request

        Args:
            request(dict): request data
        """
        return cls(**request)

    @classmethod
    def from_query(cls, query_data):
        """
        Recover existed FuturesPosition from query data

        Args:
            query_data(dict): query data
        """
        position = cls(**query_data)
        # position.long_cost = position.total_long_cost / position.multiplier / position.long_amount if \
        #     position.long_amount else 0
        # position.short_cost = position.total_short_cost / position.multiplier / position.short_amount if\
        #     position.short_amount else 0
        return position

    def to_database_item(self):
        """
        To redis item
        """
        redis_item = {
            'symbol': self.symbol,
            'price': self.price,
            'long_amount': self.long_amount,
            'short_amount': self.short_amount,
            'long_margin': self.long_margin,
            'short_margin': self.short_margin,
            'long_cost': self.long_cost,
            'short_cost': self.short_cost,
            'value': self.value,
            'profit': self.profit,
            'today_long_open': self.today_long_open,
            'today_short_open': self.today_short_open,
            'today_profit': self.today_profit
        }
        return redis_item
