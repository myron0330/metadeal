# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: PMS entity file.
#   Author: Myron
# **********************************************************************************#
import time
import numpy as np
from copy import copy
from utils.linked_list import Node
from utils.datetime import (
    get_next_trading_date,
    get_latest_trading_date
)
from utils.decorator import singleton
from utils.dict import *
from .. base import *
from ... configs import logger
from ... core.clock import clock
from ... core.schema import *
from ... core.enums import (
    SecuritiesType,
    SchemaType
)
from ... data.database_api import *
from ... market.market_quote import MarketQuote
from ... trade.order import (
    Order,
    OrderState,
    OrderStateMessage
)
from ... instrument.asset_service import AssetService


asset_service = AssetService()


@singleton
class FuturesPMSAgent(object):
    """
    Futures pms agent.
    """
    portfolio_info = DefaultDict(PortfolioSchema(portfolio_type='parent_portfolio'))
    sub_portfolio_info = DefaultDict(PortfolioSchema(portfolio_type='sub_portfolio'))
    position_info = DefaultDict(PositionSchema(date=clock.current_date.strftime('%Y%m%d')))
    order_info = DefaultDict(OrderSchema(date=clock.current_date.strftime('%Y%m%d')))
    trade_info = DefaultDict(TradeSchema(date=clock.current_date.strftime('%Y%m%d')))
    _current_exchangeable = []
    current_exchangeable_flag = None

    @property
    def current_exchangeable(self):
        """
        Current exchangeable futures.
        """
        cur_time = time.time()
        if not self.current_exchangeable_flag and cur_time - self.current_exchangeable_flag > 60 * 30:
            self._current_exchangeable = get_current_exchangeable_futures()
            self.current_exchangeable_flag = cur_time
        return self._current_exchangeable

    def prepare(self):
        """
        Prepare when service is loading.
        """
        pass

    def pre_trading_day(self, force_date=None):
        """
        盘前处理: load collections, synchronize portfolio, and dump info to database

        Args:
            force_date(datetime.datetime): specific a base date
        """
        date = force_date or clock.clearing_date
        message = 'Begin pre trading day: '+date.strftime('%Y-%m-%d')
        logger.info('[FUTURES] [PRE TRADING DAY]'+message)
        futures_portfolio_ids = query_portfolio_ids_by_(SecuritiesType.futures)
        delete_redis_([SchemaType.position, SchemaType.order], futures_portfolio_ids)
        position_info = query_by_ids_('mongodb', SchemaType.position, date, futures_portfolio_ids)
        invalid_portfolios = [key for key in position_info if key not in futures_portfolio_ids]
        if invalid_portfolios:
            invalid_msg = 'Position not loaded'+', '.join(invalid_portfolios)
            logger.info('[SECURITY] [PRE TRADING DAY]'+invalid_msg)
        dump_to_('redis', SchemaType.position, position_info) if position_info else None
        order_info = query_by_ids_('mongodb', SchemaType.order, date, futures_portfolio_ids)
        dump_to_('redis', SchemaType.order, order_info) if order_info else None
        self.clear()
        end_msg = 'End pre trading day: '+date.strftime('%Y-%m-%d')
        logger.info('[FUTURES] [PRE TRADING DAY]'+end_msg)

    def accept_orders(self, orders):
        """
        Interface to accept orders from outside. Things to be done: 1) do orders pre_check;
                                                                    2) dump orders to database;
                                                                    3) send valid orders to broker for transact;
        Args:
            orders(list): orders requests
        """
        if isinstance(orders, dict):
            orders = [orders]
        pms_orders = \
            [self._order_check(Order.from_request(order)) for order in orders]
        update_('redis', SchemaType.order, pms_orders, date=clock.clearing_date)
        active_pms_orders = [order for order in pms_orders if order.state in OrderState.ACTIVE]
        self.pms_broker.futures_broker.accept_orders(active_pms_orders)

    def post_trading_day(self, force_date=None):
        """
        盘后处理

        Args:
            force_date(datetime.datetime): 执行非当前日期的force_date日post_trading_day
        """
        date = force_date or clock.current_date
        msg = 'Begin post trading day: '+date.strftime('%Y-%m-%d')
        logger.info('[FUTURES] [POST TRADING DAY]'+msg)
        portfolio_info = query_portfolio_info_by_(SecuritiesType.futures)
        position_info = query_by_ids_('mongodb', SchemaType.position, date, portfolio_info.keys())
        self.close_expired_position(position_info)

        benchmark_dict = {e.portfolio_id: e.benchmark for e in portfolio_info.itervalues()}
        position_info = self.evaluate(position_info, benchmark_dict, force_date)
        self.position_info.update(position_info)
        if self.position_info:
            dump_to_('all', SchemaType.position, self.position_info)
        new_position_info = self._synchronize_position(date)
        if new_position_info:
            dump_to_('mongodb', SchemaType.position, new_position_info)
        self.clear()
        msg = 'End post trading day: '+date.strftime('%Y-%m-%d')
        logger.info('[FUTURES] [POST TRADING DAY]'+msg)

    def evaluate(self, position_info=None, force_evaluate_date=None):
        """
        计算持仓市值、浮动盈亏以及当日用户权益

        Args:
            position_info(dict): 用户持仓数据
            force_evaluate_date(datetime.datetime): 是否强制对根据该日期进行估值

        Returns:
            position_info(dict): 更新之后的持仓数据
        """
        if not position_info:
            return position_info
        if force_evaluate_date:
            latest_trading_date = get_latest_trading_date(force_evaluate_date)
            last_price_info = load_equity_market_data(latest_trading_date)
        else:
            market_quote = MarketQuote()
            last_price_info = market_quote.get_price_info(security_type=SecuritiesType.futures)

        for portfolio_id, position_schema in position_info.iteritems():
            total_position_margin = 0
            for symbol, position in position_schema.positions.iteritems():
                price_info = last_price_info.get(symbol)
                if position and price_info:
                    margin_rate, commission_obj, multiplier, min_change_price, _ = \
                        asset_service.get_future_trade_params(symbol)
                    position, float_pnl_added = position.evaluate(price_info['closePrice'],
                                                                  multiplier=multiplier,
                                                                  margin_rate=margin_rate)
                    position_schema.positions[symbol] = position
                    position_schema.portfolio_value += float_pnl_added
                total_position_margin = total_position_margin + position.long_margin + position.short_margin

            position_schema.cash = position_schema.portfolio_value - total_position_margin
            position_schema.daily_return = position_schema.portfolio_value / position_schema.pre_portfolio_value - 1
            position_info[portfolio_id] = position_schema
        return position_info

    def _order_expired_position(self, position_info):
        """
        生成所有期货账户持仓的到期平仓订单

        Args:
            position_info(dict): {portfolio_id: position_schema, ,,,}

        Returns(dict): {portfolio_id: [orders], ,,,}

        """
        def expired_positions(future_symbol):
            return not asset_service.get_asset_info(future_symbol).is_active_within(
                start=clock.current_date, exclude_last_date=True)

        # orders_to_close group by portfolio_id
        orders_to_close = DefaultDict(list())
        for portfolio_id, position_schema in position_info.iteritems():
            for symbol, position in position_schema.positions.iteritems():
                order_time = ' '.join([clock.current_date.strftime('%Y%m%d'), clock.current_minute])
                if expired_positions(position.symbol):
                    long_amount = position.long_amount
                    short_amount = position.short_amount
                    if long_amount and not np.isnan(long_amount):
                        msg = '{}: The contract {} is expiring and the system ' \
                              'closes relevant position.'.format(clock.current_date.strftime('%Y-%m-%d'), symbol)
                        logger.info('[FUTURES] [CLOSE EXPIRED]' + msg)
                        order = Order(symbol=symbol, amount=long_amount, direction=-1, offset_flag='close',
                                      portfolio_id=portfolio_id, order_time=order_time,
                                      state=OrderState.ORDER_SUBMITTED)
                        self._order_check(order)
                        orders_to_close[portfolio_id].append(order)
                    if short_amount and not np.isnan(short_amount):
                        msg = '{}: The contract {} is expiring and the system ' \
                              'closes relevant position.'.format(clock.current_date.strftime('%Y-%m-%d'), symbol)
                        logger.info('[FUTURES] [CLOSE EXPIRED]' + msg)
                        order = Order(symbol=symbol, amount=short_amount, direction=1, offset_flag='close',
                                      portfolio_id=portfolio_id, order_time=order_time,
                                      state=OrderState.ORDER_SUBMITTED)
                        self._order_check(order)
                        orders_to_close[portfolio_id].append(order)
        return orders_to_close

    def close_expired_position(self, position_info):
        """
        期货合约到期的持仓根据结算价平仓

        Args:
            position_info(dict): {portfolio_id: position_schema, ,,,}

        """
        orders_to_close = self._order_expired_position(position_info)
        if not orders_to_close:
            return
        close_orders = np.concatenate(orders_to_close.values()).tolist()
        update_('redis', SchemaType.order, close_orders, date=clock.clearing_date)

        # todo: 需用结算价
        market_quote = MarketQuote.get_instance()
        last_price_info = market_quote.get_price_info(security_type=SecuritiesType.futures)
        nodes_positions = query_from_('redis', SchemaType.position, portfolio_id=orders_to_close.keys())

        for portfolio_id, orders in orders_to_close.iteritems():
            current_position = nodes_positions.get(portfolio_id, None)
            if not current_position:
                continue
            for order in orders:
                symbol = order.symbol
                node = Node(obj=order)
                price = last_price_info.get(symbol, None)
                last_price = price['closePrice'] if price else current_position.positions[symbol].price
                bar_data = MarketBarData.mock_pre_settle_price(symbol, clock.current_minute, last_price)
                volume_ceiling = {portfolio_id: np.inf}
                changed_orders, _ = transact_futures_bar(node, bar_data, volume_ceiling, {}, nodes_positions)
                # Update changed orders to redis
                update_('all', SchemaType.order, [changed_orders], date=clock.clearing_date)

        # Dump changed positions to redis
        dump_to_('redis', SchemaType.position, nodes_positions)

    def clear(self):
        """
        Clear info
        """
        self.portfolio_info.clear()
        self.sub_portfolio_info.clear()
        self.position_info.clear()
        self.order_info.clear()
        self.trade_info.clear()

    def _synchronize_position(self, date=None):
        """
        同步昨结算持仓信息
        """
        # here is current date
        date = date or clock.current_date
        next_date = get_next_trading_date(date).strftime('%Y%m%d')

        def _update_date(key, value):
            """
            Update the date of node value
            """
            value.date = next_date
            value.pre_portfolio_value = value.portfolio_value
            value.benchmark_return = 0.
            value.daily_return = 0.
            return key, value

        return dict_map(_update_date, copy(self.position_info))

    def _order_check(self, order):
        """
        Check if the order is a valid security order

        Args:
            order(Order): order
        """
        order.order_time = ' '.join([clock.current_date.strftime('%Y%m%d'), clock.current_minute])
        if order.symbol not in self.current_exchangeable:
            order.state = OrderState.REJECTED
            order.state_message = OrderStateMessage.NINC_HALT
        if order.order_amount == 0 or not isinstance(order.order_amount, int):
            order.state = OrderState.REJECTED
            order.state_message = OrderStateMessage.INVALID_AMOUNT
        if order.offset_flag == 'close':
            available_amount = 0
            position_info = query_from_('redis', SchemaType.position,
                                        portfolio_id=order.portfolio_id).get(order.portfolio_id)
            if position_info:
                position = position_info.positions.get(order.symbol)
                if position:
                    available_amount = position.long_amount if order.direction == -1 else position.short_amount
            if order.order_amount > available_amount:
                order.state = OrderState.REJECTED
                order.state_message = OrderStateMessage.NO_ENOUGH_AMOUNT
        msg = order.__repr__()
        logger.debug('[FUTURES] [ACCEPT ORDERS]'+msg)
        return order
