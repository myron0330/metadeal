# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
#   Author: Myron
# **********************************************************************************#
import numpy as np
from copy import copy
import operator
import math
from .. base import *
from ... import logger
from ... core.enum import SecuritiesType
from ... core.clock import clock
from ... core.schema import SchemaType
from ... instrument.asset_service import AssetService
from ... trade.order import PMSOrder, OrderState, OrderStateMessage
from ... trade.cost import Commission
from ... trade.position import FuturesPosition
from ... trade.trade import PMSTrade
from ... utils.dict_utils import DefaultDict, CompositeDict
from ... utils.linked_list_utils import LinkedList, Node
from ... utils.error_utils import Errors
from ... utils.decorator_utils import mutex_lock, scramble_redis_lock
from ... database.database_api import get_futures_limit_price
from ... database.redis_base import redis_queue, RedisCollection
from ... market.market_quote import MarketQuote


asset_service = AssetService()


def real_time_evaluate(position_info=None):
    """
    对期货账户进行实时估值
    Args:
        position_info(dict): {portfolio_id: position_schema, portfolio_id: position_schema,,,}

    Returns(dict): 实时估值后的position_info

    """
    if not position_info:
        return position_info
    market_quote = MarketQuote.get_instance()
    last_price_info = market_quote.get_price_info(security_type=SecuritiesType.FUTURES)

    for portfolio_id, position_schema in position_info.iteritems():
        total_position_margin = 0
        for symbol, position in position_schema.positions.iteritems():
            price_info = last_price_info.get(symbol)
            # logger.info("Price message: {}, {}".format(symbol, price_info))
            if position and price_info:
                margin_rate, commission_obj, multiplier, min_change_price, _ = \
                    asset_service.get_future_trade_params(symbol)
                position, float_pnl_added = position.evaluate(price_info['closePrice'], multiplier, margin_rate)
                position_schema.positions[symbol] = position
                position_schema.portfolio_value += float_pnl_added
            # if price info is not available, the total value would add the latest evaluated value.
            total_position_margin = total_position_margin + position.long_margin + position.short_margin
        position_schema.cash = position_schema.portfolio_value - total_position_margin
    return position_info


class FuturesBroker(object):

    pool = DefaultDict(LinkedList)
    limit_move_price = None
    limit_move_key = None
    pre_settlement_price = None
    pre_settlement_price_key = None
    nodes_positions = DefaultDict()

    def __new__(cls, *args, **kwargs):
        """
        Single instance order pool
        """
        if not hasattr(cls, '_instance'):
            cls._instance = super(FuturesBroker, cls).__new__(cls)
        return cls._instance

    @classmethod
    def get_limit_move_price(cls):
        """
        Get st securities
        """
        if cls.limit_move_price is None or cls.limit_move_key != clock.clearing_date:
            temp_dict = get_futures_limit_price(clock.clearing_date)
            if temp_dict:
                cls.limit_move_price = temp_dict
                cls.limit_move_key = clock.clearing_date
        # if cls.limit_move_price is None:
        #     cls.limit_move_price = get_futures_limit_price(clock.previous_trading_date)
        #     cls.limit_move_key = clock.previous_trading_date
        return cls.limit_move_price

    def get_pre_settlement_price(self):
        """
        Get pre_settlement price
        """
        if self.pre_settlement_price is None or self.pre_settlement_price_key != clock.current_date:
            # todo. add pre_settlement_price cache
            self.pre_settlement_price = (lambda x: None)
            self.pre_settlement_price_key = clock.current_date
        return self.pre_settlement_price

    @scramble_redis_lock()
    def prepare(self):
        """
        load active orders into self.pool
        """
        order_today = query_by_securities_('redis', SchemaType.order, clock.clearing_date,
                                           securities_type=SecuritiesType.FUTURES)
        # active orders group by symbol, submit_time as key
        symbol_orders = CompositeDict()
        for order_schema in order_today.itervalues():
            for order in order_schema.orders.itervalues():
                if not order.order_time:
                    continue
                if order.state \
                        in OrderState.ACTIVE \
                        and order.order_time.split(' ')[0] <= clock.clearing_date.strftime('%Y-%m-%d'):
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
        self.nodes_positions = query_from_('redis', SchemaType.futures_position, portfolio_id=symbol_portfolios)

        # Transact
        volume_ceiling = {e: bar_data.total_volume for e in symbol_portfolios}
        feedback = linked_list.traversal(func=transact_futures_bar, bar_data=bar_data, volume_ceiling=volume_ceiling,
                                         limit_move_price=self.get_limit_move_price(),
                                         nodes_positions=self.nodes_positions)
        changed_orders = map(lambda x: x[0], feedback)
        original_nodes = map(lambda x: x[1], feedback)

        # Update changed orders to redis
        update_('redis', SchemaType.order, changed_orders, date=clock.clearing_date)

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

    def post_trading_day(self):
        """
        Post trading days
        """
        self.settlement()
        self.clear()

    def settlement(self):
        # pass
        # self.portfolio.settle(self)
        self.clear()

    def clear(self):
        """
        Clear info
        """
        self.pool.clear()
        self.nodes_positions.clear()


def transact_futures_bar(node, bar_data, volume_ceiling, limit_move_price, nodes_positions):
    """
    Transact using bar database

    Args:
        node(node): node
        bar_data(database): bar database
        volume_ceiling(dict): 各账户某分钟线可交易最大volume
        limit_move_price(dict): 当日各合约涨跌停价格
        nodes_positions(dict): 参与symbol撮合的组合持仓document
    """
    order = copy(node.obj)
    portfolio = nodes_positions.get(order.portfolio_id)
    if portfolio is None:
        _change_order_state(order, OrderState.ERROR, OrderStateMessage.INVALID_PORTFOLIO)
        msg = order.__repr__()
        logger.error('[TRANSACTION FAILED]'+msg)
        return order, node
    if order.state == OrderState.CANCEL_SUBMITTED:
        _change_order_state(order, OrderState.CANCELED, OrderStateMessage.CANCELED)
        return order, node
    if order.order_amount == 0:
        _change_order_state(order, OrderState.FILLED, OrderStateMessage.FILLED)
        return order, node

    volume = volume_ceiling.get(order.portfolio_id, 0)
    if volume == 0:
        _change_order_state(order, OrderState.OPEN, OrderStateMessage.NO_AMOUNT)
        return order, node
    act_price = bar_data.open_price
    act_price = order.price if order.order_type == 'limit' else act_price
    # todo: 处理涨跌停
    high_price, low_price = bar_data.high_price, bar_data.low_price
    # limit_up, limit_down = limit_move_price[order.symbol]
    margin_rate, commission_obj, multiplier, min_change_price, _ =  \
        asset_service.get_future_trade_params(order.symbol)

    if order.offset_flag == 'close':
        current_position = portfolio.positions[order.symbol]
        if not current_position:
            _change_order_state(order.order_id, OrderState.REJECTED, OrderStateMessage.SELLOUT)
            return order, node

        holding = current_position.long_amount if order.direction == -1 else current_position.short_amount
        # 持仓量、可成交量、订单委托量
        if order.open_amount > holding:
            _change_order_state(order, OrderState.ERROR, OrderStateMessage.NO_ENOUGH_CLOSE_AMOUNT)
            return order, node

        tx_amount = min(holding, volume, order.open_amount)
        market_value = act_price * multiplier
        commission = commission_obj.calculate_futures_commission(market_value, offset_flag='close') * tx_amount

    else:
        # 单手市值
        market_value = act_price * multiplier
        margin = market_value * margin_rate
        commission = commission_obj.calculate_futures_commission(market_value)

        # do position level evaluate
        portfolio = real_time_evaluate({order.portfolio_id: portfolio}).get(order.portfolio_id)
        if not sum([margin, commission]):
            max_amount = 0
        else:
            max_amount = math.floor(portfolio.cash / (margin + commission))
        # todo: 需要区分是订单下达时候的资金不足(应REJECT)，还是行情变化后的可用保证金小于开仓所需(这种应不限制)
        if order.open_amount > max_amount:
            _change_order_state(order.order_id, OrderState.REJECTED, OrderStateMessage.NO_ENOUGH_MARGIN)
            return order, node
        # 实际开仓数量、实际佣金
        tx_amount = min(volume, order.open_amount)
        commission = commission * tx_amount

    if tx_amount == 0:
        _change_order_state(order, OrderState.OPEN, OrderStateMessage.SELLOUT)
        return order, node

    order._commission = order.commission + commission
    order._filled_amount = order.filled_amount + tx_amount
    order._turnover_value = order.turnover_value + tx_amount * act_price
    # transact_price 等于加权成交价格
    order._transact_price = order.turnover_value / order.filled_amount
    order._filled_time = ' '.join([portfolio.date, bar_data.bar_minute])
    futures_trade = PMSTrade(order.order_id, order.symbol, order.direction, order.offset_flag, tx_amount,
                             act_price, order.filled_time, commission, None, order.portfolio_id)

    redis_queue.put([futures_trade.to_dict()], key=RedisCollection.trade)
    if order.filled_amount == order.order_amount:
        _change_order_state(order, OrderState.FILLED, OrderStateMessage.FILLED)
    # elif order.filled_amount == 0:
    #     _change_order_state(order, OrderState.OPEN)
    else:
        _change_order_state(order, OrderState.PARTIAL_FILLED)
    volume_after = max((volume - tx_amount), 0)
    volume_ceiling.update({order.portfolio_id: volume_after})
    _update_portfolio_by_trade(portfolio, futures_trade, multiplier, margin_rate)
    msg = order.__repr__()
    logger.debug('[FUTURES] [TRANSACTION]'+msg)
    return order, node


def _update_portfolio_by_trade(portfolio, trade, multiplier, margin_rate):
    """
    按成交更新相应账户的合约持仓及保证金账户余额，仅反映该持仓的价格所导致的盈亏变化，不更新账户可用
    Args:
        portfolio(PortfolioSchema): 当日持仓
        trade(PMSTrade): 成交
        multiplier: 合约乘数
        margin_rate: 保证金率

    # Returns: 更新成交后的portfolio
    #
    """
    symbol = trade.symbol
    current_position = portfolio.positions.get(symbol)
    if not current_position:
        current_position = FuturesPosition(trade.symbol, trade.transact_price)
        portfolio.positions[symbol] = current_position

    if not trade.filled_amount or np.isnan(trade.filled_amount):
        trade.filled_amount = 0
    offset = 1 if trade.offset_flag == 'open' else -1
    trade_mv = offset * trade.direction * trade.filled_amount * multiplier
    if trade.direction == 1:
        original_amount = current_position.long_amount
        if trade.offset_flag == 'open':
            # 更新持仓浮动盈亏
            current_position, float_pnl = current_position.evaluate(trade.transact_price, multiplier, margin_rate)
            current_position.long_amount += trade.filled_amount
            current_position.long_cost = \
                (current_position.long_cost * original_amount + trade.filled_amount * trade.transact_price) / \
                current_position.long_amount if current_position.long_amount else 0
            current_position.value += trade.transact_price * trade_mv
            portfolio.portfolio_value += float_pnl
        else:
            # 先处理成交的平仓盈亏, 再更新持仓浮动盈亏增量
            close_pnl = current_position.calc_close_pnl(trade, multiplier)
            current_position.short_amount -= trade.filled_amount
            # todo: 平仓应不更改持仓成本
            # current_position.short_cost = \
            #     (current_position.short_cost * original_amount - trade.filled_amount * trade.transact_price) / \
            #     current_position.short_amount if current_position.short_amount else 0
            current_position.value -= current_position.short_cost * trade_mv
            current_position, float_pnl = current_position.evaluate(trade.transact_price, multiplier, margin_rate)
            portfolio.portfolio_value = portfolio.portfolio_value + close_pnl + float_pnl
    else:
        original_amount = current_position.short_amount
        if trade.offset_flag == 'open':
            # 更新持仓浮动盈亏
            current_position, float_pnl = current_position.evaluate(trade.transact_price, multiplier, margin_rate)
            current_position.short_amount += trade.filled_amount
            current_position.short_cost = \
                (current_position.short_cost * original_amount + trade.filled_amount * trade.transact_price) / \
                current_position.short_amount if current_position.short_amount else 0
            current_position.value += trade.transact_price * trade_mv
            portfolio.portfolio_value += float_pnl
        else:
            # 先处理成交的平仓盈亏, 再更新持仓浮动盈亏增量
            close_pnl = current_position.calc_close_pnl(trade, multiplier)
            current_position.long_amount -= trade.filled_amount
            # todo: 平仓应不更改持仓成本
            # current_position.long_cost = \
            #     (current_position.long_cost * original_amount - trade.filled_amount * trade.transact_price) / \
            #     current_position.long_amount if current_position.long_amount else 0
            current_position.value -= current_position.long_cost * trade_mv
            current_position, float_pnl = current_position.evaluate(trade.transact_price, multiplier, margin_rate)
            portfolio.portfolio_value = portfolio.portfolio_value + close_pnl + float_pnl
    portfolio.portfolio_value -= trade.commission


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
