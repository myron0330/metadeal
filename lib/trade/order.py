# -*- coding: utf-8 -*-
import json
import numpy as np
from uuid import uuid1
from utils.error import Errors
from .. core.objects import ValueObject
from .. core.enums import SecuritiesType


def choose_order(account_type):
    """
    Choose order by account type.
    Args:
        account_type(string): account type.

    Returns:
        obj: Order object
    """
    if account_type == SecuritiesType.futures:
        order_obj = Order
    else:
        raise Errors.INVALID_ACCOUNT_TYPE
    return order_obj


class OrderStateMessage(object):
    """
    Enums: Order state message.
    """
    TO_FILL = u'待挂单'
    OPEN = u'待成交'
    UP_LIMIT = u'证券涨停'
    DOWN_LIMIT = u'证券跌停'
    PARTIAL_FILLED = u'部分成交'
    FILLED = u'全部成交'
    SELLOUT = u'无可卖头寸或现金'
    NO_AMOUNT = u'当日无成交量'
    NO_NAV = u'当日无净值'
    PRICE_UNCOVER = u'限价单价格未到'
    CANCELED = u'已撤单'
    NINC_HALT = u'证券代码不满足条件或证券停牌'
    TYPE_ERROR = u'订单类型错误'
    TO_CANCEL = u'待撤单'
    FAILED = u'系统错误'
    INVALID_PRICE = u'限价单价格越界'
    INACTIVE = u'证券已下市'
    INVALID_SYMBOL = u'下单合约非法'
    NO_ENOUGH_CASH = u'可用现金不足'
    NO_ENOUGH_MARGIN = u'可用保证金不足'
    NO_ENOUGH_AMOUNT = u'可用持仓不足'
    NO_ENOUGH_CLOSE_AMOUNT = u'可平持仓数量不足'
    NO_ENOUGH_SHARE = u'可赎回份额不足'
    INVALID_AMOUNT = u'下单数量非法'
    INVALID_PORTFOLIO = u'订单无对应组合持仓'
    REJECTED = u'拒單'


class OrderState(object):
    """
    Enums: Order state.
    """
    ORDER_SUBMITTED = 'ORDER_SUBMITTED'
    CANCEL_SUBMITTED = 'CANCEL_SUBMITTED'
    OPEN = 'OPEN'
    PARTIAL_FILLED = 'PARTIAL_FILLED'
    FILLED = 'FILLED'
    REJECTED = 'REJECTED'
    CANCELED = 'CANCELED'
    ERROR = 'ERROR'

    INACTIVE = [FILLED, REJECTED, CANCELED, ERROR]
    ACTIVE = [ORDER_SUBMITTED, CANCEL_SUBMITTED, OPEN, PARTIAL_FILLED]
    ALL = [ORDER_SUBMITTED, CANCEL_SUBMITTED, OPEN, PARTIAL_FILLED, FILLED, REJECTED, CANCELED, ERROR]


class BaseOrder(ValueObject):

    __slots__ = [
        'symbol',
        'order_amount',
        'order_time',
        'direction',
        'order_type',
        'price',
        'state',
        'state_message',
        'order_id',
        'filled_time',
        'filled_amount',
        'transact_price',
        'slippage',
        'commission'
    ]

    def __init__(self, symbol=None,
                 order_amount=None,
                 order_time=None,
                 direction=None,
                 order_type='market',
                 price=0.,
                 state=OrderState.ORDER_SUBMITTED,
                 state_message=OrderStateMessage.TO_FILL,
                 order_id=None,
                 filled_time=None,
                 filled_amount=None,
                 transact_price=None,
                 slippage=None,
                 commission=None):
        self.symbol = symbol
        self.order_amount = order_amount
        self.order_time = order_time
        self.direction = direction
        self.order_type = order_type
        self.price = price
        self.state = state
        self.state_message = state_message
        self.order_id = order_id
        self.filled_time = filled_time
        self.filled_amount = filled_amount
        self.transact_price = transact_price
        self.slippage = slippage
        self.commission = commission

    def __repr__(self):
        repr_dict = {key: unicode(value) for key, value in self.__dict__.iteritems()}
        return ''.join(['Order', json.dumps(repr_dict).replace('"_', '').replace('"', '').replace('{', '(').
                       replace('}', ')').replace('null', 'None')])


class Order(BaseOrder):
    """
    Order instance.
    """
    __cur_date__, __max_order_id__ = None, 1
    known_order_ids = set()

    __slots__ = [
        'portfolio_id',
        'order_id',
        'symbol',
        'order_amount',
        'filled_amount',
        'order_time',
        'filled_time',
        'order_type',
        'price',
        'transact_price',
        'turnover_value',
        'direction',
        'offset_flag',
        'commission',
        'slippage',
        'state',
        'state_message',
    ]

    def __getstate__(self):
        return dict(
            (slot, getattr(self, slot))
            for slot in self.__slots__
            if hasattr(self, slot)
        )

    def __setstate__(self, state):
        for slot, value in state.items():
            setattr(self, slot, value)

    def __init__(self, symbol, order_amount, order_time=None, order_type='limit', price=0.,
                 portfolio_id=None, order_id=None, offset_flag=None, direction=None,
                 **kwargs):
        super(Order, self).__init__(symbol=symbol, order_amount=order_amount, order_time=order_time,
                                    order_type=order_type, price=price)
        self.order_id = order_id if order_id is not None else self.generate_order_id(order_time=order_time)
        self.portfolio_id = portfolio_id
        self.direction = direction if direction is not None else (order_amount / abs(order_amount) if order_amount != 0 else 0)
        self.turnover_value = 0.
        self.offset_flag = offset_flag or ('open' if np.sign(order_amount) == 1 else 'close')

    @classmethod
    def generate_order_id(cls, order_time, generated_id=None):
        """
        Generate order id.
        Args:
            order_time(string): order time
            generated_id(string): generate id
        """
        _get_date_hash = (lambda x: x[:10])
        order_date = _get_date_hash(order_time)
        if order_date != cls.__cur_date__:
            cls.__cur_date__ = order_date
            cls.__max_order_id__ = 1
        if generated_id is None:
            generated_id = '%07d' % (cls.__max_order_id__,)
            while generated_id in cls.known_order_ids:
                cls.__max_order_id__ += 1
                generated_id = '%07d' % (cls.__max_order_id__,)
            cls.__max_order_id__ += 1
        cls.known_order_ids.add(generated_id)
        return generated_id

    def to_dict(self):
        """
        To dict
        """
        return self.__dict__

    @classmethod
    def from_request(cls, request):
        """
        Generate new order from request

        Args:
            request(dict): request database
        """
        return cls(**request)

    @classmethod
    def from_query(cls, query_data):
        """
        Recover existed order from query database

        Args:
            query_data(dict): query database
        """
        order = cls(**query_data)
        return order

    @property
    def __dict__(self):
        mapper = (lambda x: x.strip('_'))
        return {mapper(key): getattr(self, key) for key in self.__slots__}

    def __repr__(self):
        return ''.join(['Order', json.dumps(self.__dict__).replace('"_', '').replace('"', '').replace('{', '(').
                       replace('}', ')').replace('null', 'None')])


class DigitalCurrencyOrder(ValueObject):

    __slots__ = ['symbol',
                 'amount',
                 'order_time',
                 'order_type',
                 'price',
                 'state',
                 'state_message',
                 'order_id',
                 'account_id',
                 'filled_time',
                 'filled_amount',
                 'transact_price',
                 'fee',
                 'fee_currency',
                 'side',
                 'direction',
                 'turnover_value',
                 'exchange']

    def __init__(self, symbol, amount, order_time=None, order_type='market', price=0.,
                 state=OrderState.ORDER_SUBMITTED, state_message=OrderStateMessage.TO_FILL,
                 order_id=None, account_id=None, filled_time=None, filled_amount=None,
                 transact_price=None, fee=None, fee_currency=None,
                 side=None, direction=None, turnover_value=None, exchange=None):
        self.symbol = symbol
        self.amount = amount
        self.order_time = order_time
        self.order_type = order_type
        self.price = price
        self.state = state
        self.state_message = state_message
        self.order_id = order_id or str(uuid1())
        self.account_id = account_id
        self.filled_time = filled_time
        self.filled_amount = filled_amount
        self.transact_price = transact_price
        self.fee = fee
        self.fee_currency = fee_currency
        self.side = side or ('BUY' if np.sign(amount) == 1 else 'SELL')
        self.direction = direction or (amount/abs(amount) if amount != 0 else 0)
        self.turnover_value = turnover_value
        self.exchange = exchange

    def update_from_subscribe(self, item):
        """
        Update from subscribe.
        """
        order_state_map = {
            'PENDING_NEW': OrderState.ORDER_SUBMITTED,
            'NEW': OrderState.OPEN,
            'PARTIALLY_FILLED': OrderState.PARTIAL_FILLED,
            'FILLED': OrderState.FILLED,
            'REJECTED': OrderState.REJECTED,
            'CANCELED': OrderState.CANCELED
        }
        order_state_message_map = {
            'PENDING_NEW': OrderStateMessage.TO_FILL,
            'NEW': OrderStateMessage.OPEN,
            'PARTIALLY_FILLED': OrderStateMessage.PARTIAL_FILLED,
            'FILLED': OrderStateMessage.FILLED,
            'REJECTED': OrderStateMessage.REJECTED,
            'CANCELED': OrderStateMessage.CANCELED
        }
        self.state = order_state_map[item['orderStatus']]
        self.state_message = order_state_message_map[item['orderStatus']]
        self.exchange = item['exchange']
        self.price = item['price']
        self.filled_amount = item['filled']
        self.transact_price = item['avgPrice']
        self.fee = item['fee']
        self.fee_currency = item['feeCurrency']
        self.turnover_value = item['cost']

    def to_request(self):
        """
        To exchange request.
        """
        return {
            'orderType': self.order_type.upper(),
            'symbol': self.symbol.split('.')[0],
            'side': self.side,
            'price': self.price,
            'amount': self.amount,
            'extOrdId': self.order_id
        }

    def __repr__(self):
        content = ', '.join(['{}: {{}}'.format(item) for item in self.__slots__]).format(
            self.symbol, self.amount, self.order_time, self.order_type, self.price, self.state,
            self.state_message.encode('utf-8'), self.order_id, self.account_id, self.filled_time,
            self.filled_amount, self.transact_price, self.fee, self.fee_currency, self.side,
            self.direction, self.turnover_value, self.exchange
        )
        return 'DigitalCurrencyOrder({})'.format(content)
