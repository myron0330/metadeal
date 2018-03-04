# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
#   Author: Myron
# **********************************************************************************#
import operator
import numpy as np
from copy import copy
from .. base import *
from ... import logger
from ... core.clock import clock
from ... core.schema import SchemaType
from ... core.enum import SecuritiesType
from ... trade.order import PMSOrder, OrderState, OrderStateMessage
from ... trade.cost import Commission
from ... trade.position import PMSPosition
from ... trade.trade import PMSTrade
from ... utils.dict_utils import DefaultDict, CompositeDict
from ... utils.linked_list_utils import LinkedList, Node
from ... utils.error_utils import Errors
from ... utils.decorator_utils import mutex_lock, scramble_redis_lock
from ... database.database_api import load_adjust_close_price, get_current_st_securities
from ... database.redis_base import redis_queue, RedisCollection


class SecurityBroker(object):

    pool = DefaultDict(LinkedList)
    commission = Commission()
    ST = None
    ST_key = None
    pre_close_price = None
    pre_close_price_key = None
    nodes_positions = DefaultDict()

    def __new__(cls, *args, **kwargs):
        """
        Single instance security broker
        """
        if not hasattr(cls, '_instance'):
            cls._instance = super(SecurityBroker, cls).__new__(cls)
        return cls._instance

    @classmethod
    def get_st_securities(cls):
        """
        Get st securities
        """
        if cls.ST is None or cls.ST_key != clock.current_date:
            cls.ST = get_current_st_securities()
            cls.ST_key = clock.current_date
        return cls.ST

    def get_pre_close_price(self):
        """
        Get pre_close price
        """
        if self.pre_close_price is None or self.pre_close_price_key != clock.current_date:
            self.pre_close_price = load_adjust_close_price(clock.previous_trading_date)
            self.pre_close_price_key = clock.current_date
        return self.pre_close_price

    @scramble_redis_lock()
    def prepare(self):
        """
        load active orders into order pool
        """
        order_today = query_by_securities_('redis', SchemaType.order, clock.current_date,
                                           securities_type=SecuritiesType.SECURITY)
        # active orders group by symbol, submit_time as key
        symbol_orders = CompositeDict()
        for order_schema in order_today.itervalues():
            for order in order_schema.orders.itervalues():
                if not order.order_time:
                    continue
                if order.state \
                        in OrderState.ACTIVE \
                        and order.order_time.split(' ')[0] == clock.current_date.strftime('%Y-%m-%d'):
                    symbol_orders[order.symbol].update({order: order.order_time})
        for orders in symbol_orders.itervalues():
            # order by order time
            order_list = sorted(orders.items(), key=operator.itemgetter(1))
            self.accept_orders([e[0] for e in order_list])

    @mutex_lock
    def accept_orders(self, orders):
        """
        Accept orders
        """
        if isinstance(orders, PMSOrder):
            orders = [orders]
        for order in orders:
            current_order_node = Node(order)
            self.pool[order.symbol].append(current_order_node)

    @mutex_lock
    def transact_minute(self, bar_data):
        """
        Transact for orders by minute bar database
        """
        linked_list = self.pool[bar_data.security_id]
        if not linked_list.get_length():
            return

        # Query chosen positions from redis
        symbol_portfolios = linked_list.recursive(formula=(lambda x, y: x | y),
                                                  formatter=(lambda x: {x.obj.portfolio_id}))
        self.nodes_positions = query_from_('redis', SchemaType.position, portfolio_id=symbol_portfolios)

        # Transact
        volume_ceiling = {e: bar_data.total_volume*100 for e in symbol_portfolios}
        feedback = linked_list.traversal(func=_transact_security_bar, bar_data=bar_data, volume_ceiling=volume_ceiling,
                                         pre_close_price=self.get_pre_close_price(),
                                         nodes_positions=self.nodes_positions)
        changed_orders = map(lambda x: x[0], feedback)
        original_nodes = map(lambda x: x[1], feedback)

        # Update changed orders to redis
        update_('redis', SchemaType.order, changed_orders)

        # del zero amount position
        for position_schema in self.nodes_positions.itervalues():
            if position_schema.positions.get(bar_data.security_id) == 0:
                del position_schema.positions[bar_data.security_id]

        # Dump changed positions to redis
        dump_to_('redis', SchemaType.position, self.nodes_positions)
        self.nodes_positions.clear()

        # Remove inactive orders from linked list
        active_index = [index for index, order in enumerate(changed_orders)
                        if order.state in OrderState.ACTIVE]
        active_orders = list(np.array(changed_orders)[active_index])
        active_nodes = list(np.array(original_nodes)[active_index])
        for node in set(original_nodes) - set(active_nodes):
            linked_list.delete(node)
        linked_list.traversal(func=_synchronize_node, orders=active_orders)

    @classmethod
    def calculate_stock_commission(cls, price, direction):
        """
        Calculate stock commission.

        Args:
            price(float): price
            direction(int): direction
        """
        return cls.commission.calculate_stock_commission(price, direction)

    def post_trading_day(self):
        """
        Post trading days
        """
        self.clear()

    def clear(self):
        """
        Clear info
        """
        self.pool.clear()
        self.nodes_positions.clear()

    @staticmethod
    def playback_daily(order_schema, position_schema, daily_data):
        """
        Transact daily database

        Args:
            order_schema(obj): order schema
            position_schema(obj): position schema
            daily_data(dict): daily database

        Returns:
            (obj, obj): order, position
        """
        return _playback_security_daily(order_schema, position_schema, daily_data)


def _transact_security_bar(node, bar_data, volume_ceiling, pre_close_price, nodes_positions):
    """
    Transact using bar database

    Args:
        node(node): node
        bar_data(database): bar database
    """
    order = copy(node.obj)
    portfolio = nodes_positions.get(order.portfolio_id)
    if portfolio is None:
        _change_order_state(order, OrderState.ERROR, OrderStateMessage.INVALID_PORTFOLIO)
        msg = order.__repr__()
        logger.error('[SECURITY] [TRANSACTION FAILED]'+msg)
        return order, node
    if order.state == OrderState.CANCEL_SUBMITTED:
        _change_order_state(order, OrderState.CANCELED, OrderStateMessage.CANCELED)
        return order, node
    if order.order_amount == 0:
        _change_order_state(order, OrderState.FILLED, OrderStateMessage.FILLED)
        return order, node

    act_price = bar_data.open_price
    pre_close_price = pre_close_price.get(order.symbol) or act_price
    high_price = bar_data.high_price
    low_price = bar_data.low_price
    volume = volume_ceiling.get(order.portfolio_id, 0)
    if volume == 0:
        _change_order_state(order, OrderState.OPEN, OrderStateMessage.NO_AMOUNT)
        return order, node
    limit_pct = 0.0483 if order.symbol in SecurityBroker.get_st_securities() else 0.0993
    if order.direction == 1 and portfolio.cash < act_price * order.order_amount:
        _change_order_state(order, OrderState.REJECTED, OrderStateMessage.NO_ENOUGH_CASH)
        return order, node

    # order.order_type is fixed equal to 'market' currently
    if order.order_type != 'market':
        raise Errors.INVALID_ORDER_TYPE
    commission = order.direction * SecurityBroker.calculate_stock_commission(act_price, order.direction)
    cost_price = act_price + commission
    if order.direction > 0:
        if high_price == low_price and high_price / pre_close_price > 1 + limit_pct:
            _change_order_state(order, target_message=OrderStateMessage.UP_LIMIT)
            return order, node   # 全分钟涨停

        available = \
            min(order.open_amount, volume, portfolio.cash / cost_price) * 1. if cost_price > 0 else 0
        available = int(available) // 100 * 100
    else:
        if order.symbol not in portfolio.positions:
            _change_order_state(order, target_message=OrderStateMessage.SELLOUT)
            return order, node

        if high_price == low_price and high_price / pre_close_price < 1 - limit_pct:
            _change_order_state(order, target_message=OrderStateMessage.DOWN_LIMIT)
            return order, node   # 全分钟跌停

        symbol_available_amount = portfolio.positions[order.symbol].available_amount
        available = min(order.open_amount, volume, symbol_available_amount)
        available = int(available)
        symbol_available_amount -= available

    if available == 0:
        _change_order_state(order, OrderState.OPEN, OrderStateMessage.SELLOUT)
        return order, node

    order._filled_time = ' '.join([portfolio.date, bar_data.bar_minute])
    order._transact_price = (act_price * available + order.transact_price * order.filled_amount) / \
                            (order.filled_amount + available)
    order._filled_amount = order.filled_amount + available
    order._commission = order.commission + abs(available * commission)
    order._turnover_value = order.turnover_value + available * act_price
    pms_trade = PMSTrade(order.order_id, order.symbol, order.direction, order.offset_flag, available,
                         act_price, order.filled_time, commission, None, order.portfolio_id)
    redis_queue.put([pms_trade.to_dict()], key=RedisCollection.trade)
    portfolio.cash -= order.direction * cost_price * available
    if order.filled_amount == order.order_amount:
        _change_order_state(order, OrderState.FILLED, OrderStateMessage.FILLED)
    elif order.filled_amount == 0:
        _change_order_state(order, OrderState.OPEN)
    else:
        _change_order_state(order, OrderState.PARTIAL_FILLED)

    volume_after = max((volume - available), 0)
    volume_ceiling.update({order.portfolio_id: volume_after})
    position = portfolio.positions.get(order.symbol)
    pre_amount = 0 if position is None else position.amount
    if pre_amount:
        position.amount += order.direction * available
        if order.direction == -1:
            position.total_cost = position.total_cost * position.amount / pre_amount if position.amount != 0 else 0
            position.available_amount -= available
        else:
            position.total_cost = position.total_cost + order.direction * available * cost_price
        position.cost = position.total_cost / position.amount if position.amount else 0
    else:
        if position is None:
            position = PMSPosition(order.symbol, 0, 0, 0.0)
            portfolio.positions[order.symbol] = position
        position.amount = order.direction * available
        position.total_cost = order.turnover_value + order.commission
        position.cost = position.total_cost / position.amount
    position.value = act_price * position.amount
    msg = order.__repr__()
    logger.debug('[SECURITY] [TRANSACTION]'+msg)
    return order, node


def _playback_security_daily(order_schema, position_schema, daily_data):
    """
    Transact securities by daily database

    Args:
        order_schema(obj): order schema
        position_schema(obj): position schema
        daily_data(dict): daily database

    Returns:
        (obj, obj): order, position
    """
    # todo. transact order_schema based on position_schema
    playback_order_schema, playback_position_schema = order_schema, position_schema
    return playback_order_schema, playback_position_schema


def _synchronize_node(node, orders):
    """
    Synchronize orders to linked list

    Args:
        node(node): node
        orders(list): orders
    """
    node.obj = orders.pop(0)


def _change_order_state(order, target_state=None, target_message=None):
    """
    更新当前order状态，暂不处理非active的Order

    Args:
        order (PMSOrder): 需要更新的order
        target_state (str): 目标状态
        target_message (str): 目标状态提示

    """
    if order is None:
        return
    if not isinstance(order, PMSOrder):
        raise Errors.INVALID_ORDER_OBJECT
    if target_state is not None:
        order._state = target_state
    if target_message is not None:
        order._state_message = target_message
