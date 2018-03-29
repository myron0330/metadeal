# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
import time
from uuid import uuid1
from utils.error_utils import Errors
from . objects import ValueObject
from . enums import (
    SecuritiesType,
    SchemaType
)
from .. const import (
    DEFAULT_USER_NAME,
    DEFAULT_CAPITAL_BASE,
    DEFAULT_PORTFOLIO_VALUE_BASE,
    DEFAULT_BENCHMARK
)
from .. trade.order import Order
from .. trade.trade import MetaTrade
from .. trade.position import (
    MetaPosition,
    LongShortPosition
)


def _encoding_base_info(base_info):
    """
    Encoding base info

    Args:
        base_info(dict): base info
    """
    encoded_info = list()
    for symbol, base in base_info.iteritems():
        item = {
            'symbol': symbol,
            'base': base
        }
        encoded_info.append(item)
    return encoded_info


def _decoding_base_info(encoded_info):
    """
    Decode base info

    Args:
        encoded_info(list or dict): encoded base info
    """
    if isinstance(encoded_info, dict):
        return encoded_info
    base_info = dict()
    for item in encoded_info:
        base_info[item['symbol']] = item['base']
    return base_info


class PortfolioSchema(ValueObject):
    """
    PMS portfolio
    """
    __slots__ = [
        'username',
        'portfolio_id',
        'portfolio_name',
        'portfolio_type',
        'account_type',
        'capital_base',
        'portfolio_value_base',
        'position_base',
        'cost_base',
        'sub_portfolio_ids',
        'parent_portfolio_id',
        'benchmark',
        'submit_time',
        'benchmark_base',
        'start_time',
        'delete_flag',
    ]

    def __init__(self, username=DEFAULT_USER_NAME,
                 portfolio_id=None, portfolio_name='Default portfolio',
                 portfolio_type='parent_portfolio', account_type='mix',
                 capital_base=None,
                 portfolio_value_base=DEFAULT_PORTFOLIO_VALUE_BASE,
                 position_base=None, cost_base=None,
                 sub_portfolio_ids=None, parent_portfolio_id=None,
                 delete_flag=False, benchmark=DEFAULT_BENCHMARK,
                 submit_time=None, benchmark_base=None, start_time=None):
        self.username = username
        self.portfolio_id = portfolio_id or str(uuid1())
        self.portfolio_name = portfolio_name
        self.portfolio_type = portfolio_type
        self.account_type = account_type
        self.capital_base = capital_base if capital_base is not None else DEFAULT_CAPITAL_BASE
        self.position_base = position_base or dict()
        self.cost_base = cost_base or dict()
        self.portfolio_value_base = \
            portfolio_value_base if portfolio_value_base is not None else \
            self.capital_base + sum([amount * self.cost_base.get(symbol, 0.)
                                     for symbol, amount in self.position_base.iteritems()])
        self.sub_portfolio_ids = sub_portfolio_ids or list()
        self.parent_portfolio_id = parent_portfolio_id
        self.delete_flag = delete_flag
        self.benchmark = benchmark
        self.submit_time = submit_time or time.time() * 1000
        self.benchmark_base = benchmark_base
        self.start_time = start_time or self.submit_time

    @classmethod
    def from_query(cls, item):
        """
        Generate from database item

        Args:
            item(dict): query database
        """
        item['position_base'] = _decoding_base_info(item['position_base'])
        item['cost_base'] = _decoding_base_info(item['cost_base'])
        return cls(**item)

    def to_mongodb_item(self):
        """
        To mongodb item
        """
        return (
            {
                'username': self.username,
                'portfolio_id': self.portfolio_id
            },
            {
                '$set':
                    {
                        'portfolio_name': self.portfolio_name,
                        'portfolio_type': self.portfolio_type,
                        'account_type': self.account_type,
                        'capital_base': self.capital_base,
                        'portfolio_value_base': self.portfolio_value_base,
                        'position_base': _encoding_base_info(self.position_base),
                        'cost_base': _encoding_base_info(self.cost_base),
                        'sub_portfolio_ids': self.sub_portfolio_ids,
                        'parent_portfolio_id': self.parent_portfolio_id,
                        'delete_flag': self.delete_flag,
                        'benchmark': self.benchmark,
                        'submit_time': self.submit_time,
                        'benchmark_base': self.benchmark_base,
                        'start_time': self.start_time,
                    }
            }
        )


class PositionSchema(ValueObject):
    """
    PMS position information
    """
    __slots__ = [
        'portfolio_id',
        'date',
        'cash',
        'positions',
        'pre_portfolio_value',
        'portfolio_value',
        'benchmark_return',
        'daily_return'
    ]

    def __init__(self, portfolio_id=None, date='', cash=0., positions=None,
                 pre_portfolio_value=0., portfolio_value=0.,
                 benchmark_return=0., daily_return=0.):
        self.portfolio_id = portfolio_id
        self.date = date
        self.cash = cash
        self.positions = positions if positions else dict()
        self.pre_portfolio_value = pre_portfolio_value
        self.portfolio_value = portfolio_value
        self.benchmark_return = benchmark_return
        self.daily_return = daily_return

    @classmethod
    def from_query(cls, item, securities_type=SecuritiesType.SECURITY):
        """
        Generate from query item

        Args:
            item(dict): query database
            securities_type(string): securities type
        """
        portfolio_id = item['portfolio_id']
        date = item['date']
        cash = item['cash']
        position_object = MetaPosition if securities_type == SecuritiesType.SECURITY else LongShortPosition
        positions = {position['symbol']: position_object.from_query(position) for position in item['positions']}
        pre_portfolio_value = item['pre_portfolio_value']
        portfolio_value = item['portfolio_value']
        benchmark_return = item['benchmark_return']
        daily_return = item['daily_return']
        return cls(portfolio_id=portfolio_id, date=date, cash=cash,
                   positions=positions, portfolio_value=portfolio_value,
                   pre_portfolio_value=pre_portfolio_value,
                   benchmark_return=benchmark_return, daily_return=daily_return)

    def to_redis_item(self):
        """
        To redis item
        """
        return {
            'portfolio_id': self.portfolio_id,
            'date': self.date,
            'cash': self.cash,
            'positions': [self.positions[symbol].to_database_item()
                          for symbol in sorted(self.positions.iterkeys())],
            'pre_portfolio_value': self.pre_portfolio_value,
            'portfolio_value': self.portfolio_value,
            'benchmark_return': self.benchmark_return,
            'daily_return': self.daily_return
        }

    def to_mongodb_item(self):
        """
        To mongodb item
        """
        return (
            {
                'portfolio_id': self.portfolio_id,
                'date': self.date,
            },
            {
                '$set': {
                    'cash': self.cash,
                    'pre_portfolio_value': self.pre_portfolio_value,
                    'portfolio_value': self.portfolio_value,
                    'positions': [self.positions[symbol].to_database_item()
                                  for symbol in sorted(self.positions.iterkeys())],
                    'benchmark_return': self.benchmark_return,
                    'daily_return': self.daily_return
                }
            }
        )


class OrderSchema(ValueObject):
    """
    PMS order
    """
    __slots__ = [
        'portfolio_id',
        'date',
        'orders'
    ]

    def __init__(self, portfolio_id=None, date=None, orders=None):
        """
        Args:
            portfolio_id(string): portfolio id
            date(str): date
            orders(dict of Order): orders
        """
        self.portfolio_id = portfolio_id
        self.date = date
        self.orders = orders or dict()

    @classmethod
    def from_query(cls, item):
        """
        Generate from query item

        Args:
            item(dict): query database
        """
        portfolio_id = item['portfolio_id']
        date = item['date']
        orders = dict()
        if isinstance(item['orders'], (tuple, list)):
            for order in item['orders']:
                order_id = order['order_id']
                order['portfolio_id'] = portfolio_id
                orders[order_id] = Order.from_query(order)
        else:
            for order_id, order in item['orders'].iteritems():
                order['portfolio_id'] = portfolio_id
                orders[order_id] = Order.from_query(order)
        return cls(portfolio_id=portfolio_id, date=date, orders=orders)

    def to_redis_item(self):
        """
        To redis item
        """
        return {
            'portfolio_id': self.portfolio_id,
            'date': self.date,
            'orders': {
                order_id: order.to_dict() for order_id, order in self.orders.iteritems()
            }
        }

    def to_mongodb_item(self):
        """
        To mongodb item
        """
        return (
            {
                'portfolio_id': self.portfolio_id,
                'date': self.date,
            },
            {
                '$set':
                    {'{}.{}'.format('orders', order_id): order.to_dict() for order_id, order in self.orders.iteritems()}
            }
        )


class TradeSchema(ValueObject):
    """
    PMS trade schema
    """
    __slots__ = [
        'portfolio_id',
        'date',
        'trades'
    ]

    def __init__(self, portfolio_id=None, date='', trades=list()):
        self.portfolio_id = portfolio_id
        self.date = date
        self.trades = trades

    @classmethod
    def from_query(cls, item):
        """
        Generate from query item

        Args:
            item(dict): query database
        """
        portfolio_id = item['portfolio_id']
        date = item['date']
        trades = [MetaTrade.from_query(trade) for trade in item['trades']]
        return cls(portfolio_id=portfolio_id, date=date, trades=trades)

    def to_mongodb_item(self):
        """
        To mongodb item
        """
        return (
            {
                'portfolio_id': self.portfolio_id,
                'date': self.date,
            },
            {
                '$push': {'trades': {'$each': [trade.to_dict() for trade in self.trades]}}
            }
        )


def switch_schema(schema_type):
    """
    Switch schema
    Args:
        schema_type(string): schema type

    Returns:
        schema(obj): schema
    """
    if schema_type == SchemaType.portfolio:
        schema = PortfolioSchema
    elif schema_type == SchemaType.order:
        schema = OrderSchema
    elif schema_type == SchemaType.position:
        schema = PositionSchema
    elif schema_type == SchemaType.trade:
        schema = TradeSchema
    else:
        raise Errors.INVALID_SCHEMA_TYPE
    return schema


__all__ = [
    'PortfolioSchema',
    'OrderSchema',
    'PositionSchema',
    'TradeSchema',
    'switch_schema'
]
