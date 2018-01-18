# -*- coding: utf-8 -*-
import json
import numpy as np
from .. const import (
    DEFAULT_FILLED_AMOUNT,
    DEFAULT_FILLED_TIME,
    DEFAULT_TRANSACT_PRICE,
)
from uuid import uuid1


def _get_date_hash(date):
    return date[:10]


class OrderStateMessage(object):
    """
    枚举类：订单状态信息
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


class OrderState(object):
    """
    枚举类: 订单状态
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


class OrderStatus(object):
    """
    枚举类: 订单状态
    """
    TO_FILL = OrderState.ORDER_SUBMITTED
    TO_CANCEL = OrderState.CANCEL_SUBMITTED
    FILLED = OrderState.FILLED
    CANCELLED = OrderState.CANCELED
    OPEN = OrderState.ACTIVE
    CLOSED = [OrderState.CANCELED, OrderState.FILLED, OrderState.REJECTED]
    INVALID = OrderState.ERROR
    ALL = OrderState.ALL


class BaseOrder(object):

    def __init__(self, symbol, order_amount, order_time=None, direction=None, order_type='market', price=0.,
                 order_id=None, state=OrderState.ORDER_SUBMITTED):
        self._symbol = symbol
        self._order_amount = abs(order_amount)
        self._order_time = order_time
        self._direction = direction
        self._order_type = order_type
        self._price = price
        self._state = state
        self._state_message = OrderStateMessage.TO_FILL
        self._order_id = order_id
        self._filled_time = DEFAULT_FILLED_TIME
        self._filled_amount = DEFAULT_FILLED_AMOUNT
        self._transact_price = DEFAULT_TRANSACT_PRICE
        self._slippage = 0.
        self._commission = 0.

    @property
    def symbol(self):
        return self._symbol

    @property
    def order_amount(self):
        return self._order_amount

    @property
    def order_time(self):
        return self._order_time

    @property
    def direction(self):
        return self._direction

    @property
    def price(self):
        return self._price

    @property
    def state(self):
        return self._state

    @property
    def state_message(self):
        return self._state_message

    @property
    def order_id(self):
        return self._order_id

    @property
    def filled_time(self):
        return self._filled_time

    @property
    def filled_amount(self):
        return self._filled_amount

    @property
    def transact_price(self):
        return self._transact_price

    @property
    def open_amount(self):
        """指令的未成交数量"""
        return self._order_amount - self._filled_amount

    @property
    def commission(self):
        return self._commission

    @property
    def slippage(self):
        return self._slippage

    @symbol.setter
    def symbol(self, *args):
        raise AttributeError('Exception in "BaseOrder.symbol": user must not modify order.symbol!')

    @order_amount.setter
    def order_amount(self, *args):
        raise AttributeError('Exception in "BaseOrder.order_amount": user must not modify order.order_amount!')

    @order_time.setter
    def order_time(self, *args):
        raise AttributeError('Exception in "BaseOrder.order_time": user must not modify order.order_time!')

    @direction.setter
    def direction(self, *args):
        raise AttributeError('Exception in "BaseOrder.direction": user must not modify order.direction!')

    @price.setter
    def price(self, *args):
        raise AttributeError('Exception in "BaseOrder.price": User must not modify order.price!')

    @state.setter
    def state(self, *args):
        raise AttributeError('Exception in "BaseOrder.state": user must not modify order.state!')

    @state_message.setter
    def state_message(self, *args):
        raise AttributeError('Exception in "BaseOrder.state_message": user must not modify order.state_message!')

    @order_id.setter
    def order_id(self, *args):
        raise AttributeError('Exception in "BaseOrder.order_id": User must not modify order.order_id!')

    @filled_time.setter
    def filled_time(self, *args):
        raise AttributeError('Exception in "BaseOrder.filled_time": user must not modify order.filled_time!')

    @filled_amount.setter
    def filled_amount(self, *args):
        raise AttributeError('Exception in "BaseOrder.filled_amount": user must not modify order.filled_amount!')

    @transact_price.setter
    def transact_price(self, *args):
        raise AttributeError('Exception in "BaseOrder.transact_price": User must not modify order.transact_price!')

    @commission.setter
    def commission(self, *args):
        raise AttributeError('Exception in "Order.commission": User must not modify order.commission!')

    @slippage.setter
    def slippage(self, *args):
        raise AttributeError('Exception in "Order.slippage": User must not modify Order.slippage!')

    def __repr__(self):
        repr_dict = {key: unicode(value) for key, value in self.__dict__.iteritems()}
        return ''.join(['Order', json.dumps(repr_dict).replace('"_', '').replace('"', '').replace('{', '(').
                       replace('}', ')').replace('null', 'None')])


class PMSOrder(BaseOrder):
    """
    交易指令，包含如下属性

    * self.order_id: 订单的唯一标识符
    * self.order_time：指令下达时间
    * self.symbol：指令涉及的证券代码
    * self.direction：指令方向，正为买入，负为卖出
    * self.order_amount：指令交易数量
    * self.type：指令种类，如'market'表示按市价成交，'limit'表示按限价成交
    * self.filled_time：指令成交时间
    * self.filled_amount：指令成交数量
    * self.slippage: 指令成交滑点
    * self.commission: 指令成交手续费
    * self.transact_price：指令成交价格（包含滑点）
    * self.price: 指令限价
    * self.state: 指令状态，见OrderStatus
    """
    __cur_date__, __max_order_id__ = None, 1

    __slots__ = [
        '_portfolio_id',
        '_order_id',
        '_symbol',
        '_order_amount',
        '_filled_amount',
        '_order_time',
        '_filled_time',
        '_order_type',
        '_price',
        '_transact_price',
        '_turnover_value',
        '_direction',
        '_offset_flag',
        '_commission',
        '_slippage',
        '_state',
        '_state_message',
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

    def __init__(self, symbol, amount, order_time=None, order_type='market', price=0.,
                 portfolio_id=None, order_id=None, offset_flag=None, direction=None,
                 **kwargs):
        """
        股票订单初始化

        Args:
            symbol (str): 证券合约代码，必须包含后缀，其中上证证券为.XSHG，深证证券为.XSHE
            amount (int): 委托数量，符号表示交易方向，正为买入，负为卖出
            order_time (str): optional, 订单委托时间
            order_type (str): optional, 订单委托类型，可以是'market'或'limit'
            price (float): optional, 限价单价格委托接口
            direction(int): direction
        """
        super(PMSOrder, self).__init__(symbol=symbol, order_amount=amount, order_time=order_time,
                                       order_type=order_type, price=price)
        self._order_id = order_id if order_id is not None else str(uuid1())
        self._portfolio_id = portfolio_id
        self._direction = direction if direction is not None else amount/abs(amount) if amount != 0 else 0
        self._turnover_value = 0.
        self._offset_flag = offset_flag or 'open' if np.sign(amount) == 1 else 'close'

    @property
    def offset_flag(self):
        return self._offset_flag

    @property
    def order_type(self):
        return self._order_type

    @property
    def turnover_value(self):
        return self._turnover_value

    @property
    def portfolio_id(self):
        return self._portfolio_id

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
        query_data['amount'] = query_data.pop('order_amount')
        order = cls(**query_data)
        order._filled_time = query_data['filled_time']
        order._state = query_data['state']
        order._state_message = query_data['state_message']
        order._commission = query_data['commission']
        order._slippage = query_data['slippage']
        order._turnover_value = query_data['turnover_value']
        order._filled_amount = query_data['filled_amount']
        order._transact_price = query_data['transact_price']
        order._direction = query_data.get('direction', 1)
        return order

    # def to_redis_item(self):
    #     """
    #     To redis item
    #     """
    #     return self.__dict__
    #
    # def to_mongodb_item(self):
    #     """
    #     To mongodb item
    #     """
    #     return (
    #         {
    #             'portfolio_id': self._portfolio_id,
    #             'date': _get_date_hash(self._order_time)
    #         },
    #         {
    #             '$set':
    #                 {
    #                     '%s.%s' % ('orders', self._order_id):
    #                         {
    #                             'portfolio_id': self._portfolio_id,
    #                             'order_id': self._order_id,
    #                             'symbol': self._symbol,
    #                             'order_amount': self._order_amount,
    #                             'filled_amount': self._filled_amount,
    #                             'order_time': self._order_time,
    #                             'filled_time': self._filled_time,
    #                             'order_type': self._order_type,
    #                             'price': self._price,
    #                             'transact_price': self._transact_price,
    #                             'turnover_value': self._turnover_value,
    #                             'commission': self._commission,
    #                             'slippage': self._slippage,
    #                             'direction': self._direction,
    #                             'offset_flag': self._offset_flag,
    #                             'state': self._state,
    #                             'state_message': self._state_message
    #                         }
    #                 }
    #         }
    #     )

    @property
    def __dict__(self):
        mapper = (lambda x: x.strip('_'))
        return {mapper(key): getattr(self, key) for key in self.__slots__}

    def __repr__(self):
        return ''.join(['Order', json.dumps(self.__dict__).replace('"_', '').replace('"', '').replace('{', '(').
                       replace('}', ')').replace('null', 'None')])
