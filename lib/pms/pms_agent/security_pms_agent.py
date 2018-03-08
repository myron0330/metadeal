# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: PMS entity file.
#   Author: Myron
# **********************************************************************************#
import pandas as pd
from copy import copy
from .. base import *
from .. pms_broker import PMSBroker
from ... import logger
from ... core.clock import clock
from ... core.schema import *
from ... core.enum import SecuritiesType
from lib.gateway.subscriber import MarketQuote
from ... trade.dividend import Dividend
from ... trade.order import PMSOrder, OrderState, OrderStateMessage
from ... utils.date_utils import get_next_date, get_latest_trading_date
from ... utils.dict_utils import DefaultDict, dict_map, CompositeDict


class SecurityPMSAgent(object):
    """
    Security pms pms_agent
    """
    pms_broker = PMSBroker()
    portfolio_info = DefaultDict(PortfolioSchema(portfolio_type='parent_portfolio'))
    sub_portfolio_info = DefaultDict(PortfolioSchema(portfolio_type='sub_portfolio'))
    position_info = DefaultDict(PositionSchema(date=clock.current_date.strftime('%Y%m%d')))
    order_info = DefaultDict(OrderSchema(date=clock.current_date.strftime('%Y%m%d')))
    trade_info = DefaultDict(TradeSchema(date=clock.current_date.strftime('%Y%m%d')))
    dividends = None
    current_exchangeable = []

    def __new__(cls, **kwargs):
        """
        Args:
            portfolio_info(dict): all portfolios | {portfolio_id: PortfolioSchema}
            sub_portfolio_info(dict): all sub portfolios | {sub_portfolio_id: PortfolioSchema}
            position_info(dict): all pms equities | {sub_portfolio_id: PositionSchema}
            order_info(dict): all pms orders | {sub_portfolio_id: OrderSchema}
            trade_info(dict): all pms trades | {sub_portfolio_id: TradeSchema}
        """
        if not hasattr(cls, '_instance'):
            cls._instance = super(SecurityPMSAgent, cls).__new__(cls)
        return cls._instance

    def prepare(self):
        """
        Prepare when service is loading.
        """
        self.current_exchangeable = get_current_exchangeable_securities()

    def pre_trading_day(self, with_dividend=True, force_date=None):
        """
        盘前处理: load collections, synchronize portfolio, and dump info to database

        Args:
            with_dividend(boolean): 是否分红处理
            force_date(datetime.datetime): specific a base date
        """
        date = force_date or clock.current_date
        message = 'Begin pre trading day: '+date.strftime('%Y-%m-%d')
        logger.info('[SECURITY] [PRE TRADING DAY]'+message)
        portfolio_info = query_portfolio_info_by_(SecuritiesType.SECURITY)
        portfolio_ids = portfolio_info.keys()
        delete_redis_([SchemaType.position, SchemaType.order], portfolio_ids)
        position_info = query_by_ids_('mongodb', SchemaType.position, date, portfolio_ids)
        self.position_info.update(position_info)
        invalid_portfolios = [key for key in self.position_info if key not in portfolio_info]
        if invalid_portfolios:
            invalid_msg = 'Position not loaded'+', '.join(invalid_portfolios)
            logger.info('[SECURITY] [PRE TRADING DAY]'+invalid_msg)
        self._load_dividend([date])
        self._execute_dividend(date) if with_dividend else None
        dump_to_('all', SchemaType.position, self.position_info) if self.position_info else None
        order_info = query_by_ids_('mongodb', SchemaType.order, date, portfolio_ids)
        dump_to_('redis', SchemaType.order, order_info) if order_info else None
        self.current_exchangeable = get_current_exchangeable_securities()
        self.clear()
        end_msg = 'End pre trading day: '+date.strftime('%Y-%m-%d')
        logger.info('[SECURITY] [PRE TRADING DAY]'+end_msg)

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
            [self._order_check(PMSOrder.from_request(order)) for order in orders]
        update_('redis', SchemaType.order, pms_orders)
        active_pms_orders = [order for order in pms_orders if order.state in OrderState.ACTIVE]
        self.pms_broker.security_broker.accept_orders(active_pms_orders)

    def post_trading_day(self, with_dividend=True, force_date=None):
        """
        盘后处理

        Args:
            with_dividend(Boolean): 是否执行分红
            force_date(datetime.datetime): 执行非当前日期的force_date日post_trading_day
        """
        date = force_date or clock.current_date
        msg = 'Begin post trading day: '+date.strftime('%Y-%m-%d')
        logger.info('[SECURITY] [POST TRADING DAY]'+msg)
        portfolio_info = query_portfolio_info_by_(SecuritiesType.SECURITY)
        position_info = query_by_ids_('mongodb', SchemaType.position,
                                      date=date, portfolio_ids=portfolio_info.keys())
        benchmark_dict = {e.portfolio_id: e.benchmark for e in portfolio_info.itervalues()}
        position_info = self.evaluate(position_info, benchmark_dict, force_date)
        self.position_info.update(position_info)
        self._load_dividend([date])
        if with_dividend:
            self._record_dividend(date)
        if self.position_info:
            dump_to_('all', SchemaType.position, self.position_info)
        new_position_info = self._synchronize_position(date)
        if new_position_info:
            dump_to_('mongodb', SchemaType.position, new_position_info)
        self.clear()
        msg = 'End post trading day: '+date.strftime('%Y-%m-%d')
        logger.info('[SECURITY] [POST TRADING DAY]'+msg)

    def evaluate(self, position_info=None, benchmark_dict=None, force_evaluate_date=None):
        """
        计算持仓市值、浮动盈亏以及当日用户权益

        Args:
            position_info(dict): 用户持仓数据
            force_evaluate_date(datetime.datetime): 是否强制对根据该日期进行估值
            benchmark_dict(dict): 组合所对应的benchmark

        Returns:
            position_info(dict): 更新之后的持仓数据
        """
        if not position_info:
            return position_info
        benchmark_change_percent = None
        if force_evaluate_date:
            last_price_info = load_equity_market_data(force_evaluate_date)
            # check out api.
            latest_trading_date = get_latest_trading_date(force_evaluate_date)
            benchmark_change_percent = load_change_percent_for_benchmark(latest_trading_date)
        else:
            market_quote = MarketQuote.get_instance()
            all_stocks = set(SecurityPMSAgent().current_exchangeable)
            if benchmark_dict:
                all_stocks |= set(benchmark_dict.values())
            last_price_info = market_quote.get_price_info(universes=list(all_stocks))

        # logger.info('*'*30)
        for portfolio_id, position_schema in position_info.iteritems():
            total_values = 0
            for symbol, position in position_schema.positions.iteritems():
                price_info = last_price_info.get(symbol)
                # logger.info("Price message: {}, {}".format(symbol, price_info))
                if position and price_info:
                    position.evaluate(price_info['closePrice'])
                    position_schema.positions[symbol] = position
                # if price info is not available, the total value would add the latest evaluated value.
                total_values += position.value
            # logger.info("total_values: {}".format(total_values))
            # logger.info("cash: {}".format(position_schema.cash))
            # logger.info("portfolio_value: {}".format(position_schema.portfolio_value))
            position_schema.portfolio_value = position_schema.cash + total_values
            position_schema.daily_return = position_schema.portfolio_value / position_schema.pre_portfolio_value - 1
            if benchmark_dict:
                benchmark = benchmark_dict.get(portfolio_id)
                if not benchmark:
                    position_info[portfolio_id] = position_schema
                    continue
                if force_evaluate_date:
                    position_schema.benchmark_return = benchmark_change_percent.at[benchmark]
                else:
                    benchmark_price_item = last_price_info.get(benchmark)
                    pre_benchmark_price = self.pms_broker.get_pre_close_price_of_(benchmark)
                    if benchmark_price_item and pre_benchmark_price is not None:
                        position_schema.benchmark_return = calc_return(pre_benchmark_price,
                                                                       benchmark_price_item['closePrice'])
            position_info[portfolio_id] = position_schema
        # logger.info('*'*30)
        return position_info

    def clear(self):
        """
        Clear info
        """
        self.portfolio_info.clear()
        self.sub_portfolio_info.clear()
        self.position_info.clear()
        self.order_info.clear()
        self.trade_info.clear()

    def _synchronize_position(self, date):
        """
        同步昨结算持仓信息
        """
        current_date = date or clock.current_date
        next_date = get_next_date(current_date).strftime('%Y%m%d')

        def _update_date(key, value):
            """
            Update the date of node value
            """
            value.date = next_date
            for position in value.positions.itervalues():
                position.available_amount = position.amount
            value.pre_portfolio_value = value.portfolio_value
            value.benchmark_return = 0.
            value.daily_return = 0.
            return key, value

        return dict_map(_update_date, copy(self.position_info))

    def _load_dividend(self, trading_days=None):
        """
        加载分红数据并归类
        Args:
            trading_days(list of datetime): 交易日列表

        Returns:

        """
        trading_days = trading_days or [clock.current_date]
        raw_data = load_dividend_data(trading_days)
        normalize_column = ['per_cash_div_af_tax', 'shares_bf_div', 'shares_af_div']
        raw_data[normalize_column] = raw_data[normalize_column].fillna(0).applymap(float)
        raw_data['share_ratio'] = raw_data.shares_af_div / raw_data.shares_bf_div
        result = CompositeDict()
        records = raw_data.groupby('record_date').groups
        cash_divs = raw_data.groupby('pay_cash_date').groups
        ex_divs = raw_data.groupby('ex_div_date').groups

        for date, group in records.iteritems():
            date = pd.to_datetime(date)
            temp_data = raw_data.iloc[group][['security_id', 'pay_cash_date', 'ex_div_date']].as_matrix().tolist()
            dividend_items = dict()
            for dividend_item in temp_data:
                execute_dates = filter(lambda x: x, dividend_item[1:])
                if not execute_dates:
                    continue
                execute_date = max(execute_dates)
                key = dividend_item[0]
                dividend_items[key] = execute_date
            result['div_record'][date.strftime('%Y%m%d')] = dividend_items
        result['cash_div'] = \
            {pd.to_datetime(date).strftime('%Y%m%d'): dict(
                raw_data.fillna(0).iloc[group][['security_id', 'per_cash_div_af_tax']].as_matrix())
                for date, group in cash_divs.iteritems()}
        result['share_div'] = \
            {pd.to_datetime(date).strftime('%Y%m%d'): dict(raw_data.fillna(1).iloc[group][['security_id', 'share_ratio']].as_matrix())
             for date, group in ex_divs.iteritems()}
        self.dividends = result

    def _execute_dividend(self, date=None):
        """
        分红执行
        """
        current_date = (date or clock.current_date).strftime('%Y%m%d')
        dividend_data = self.dividends
        cash_div, share_div = dividend_data.get('cash_div'), dividend_data.get('share_div')
        for position_schema in self.position_info.itervalues():
            if not isinstance(position_schema, PositionSchema):
                continue
            positions = position_schema.positions
            if cash_div and cash_div.get(current_date):
                cash_div_info = cash_div[current_date]
                for symbol, position in positions.iteritems():
                    if symbol in cash_div_info and position.dividends:
                        position_schema.cash += cash_div_info.get(symbol, 0) * position.dividends.dividend_amount
                        position.cost -= cash_div_info.get(symbol, 0)
                        logger.debug('[DIVIDEND] portfolio_id: {}, symbol: {}, cash: {}'.format(
                            position_schema.portfolio_id, symbol, cash_div_info.get(symbol, 0)))
            if share_div and share_div.get(current_date):
                share_div_info = share_div[current_date]
                for symbol, position in positions.iteritems():
                    if symbol in share_div_info and position.dividends:
                        position.amount += \
                            round(position.dividends.dividend_amount * (share_div_info.get(symbol, 1) - 1))
                        position.cost /= share_div_info.get(symbol, 1)
                        logger.debug('[DIVIDEND] portfolio_id: {}, symbol: {}, share: {}'.format(
                            position_schema.portfolio_id, symbol, share_div_info.get(symbol, 0)))

    def _record_dividend(self, date=None):
        """
        股权登记
        """
        if not self.dividends:
            return
        dividend_data = self.dividends
        current_date = (date or clock.current_date).strftime('%Y%m%d')
        div_record = dividend_data.get('div_record')
        div_record_info = div_record.get(current_date, None) if div_record else None
        if not div_record_info:
            return
        for position_schema in self.position_info.itervalues():
            positions = position_schema.positions
            for symbol, position in positions.iteritems():
                if symbol in div_record_info:
                    position.dividends = Dividend(
                        dividend_amount=position.amount,
                        expiring_date=div_record_info[symbol].strftime('%Y%m%d')) + position.dividends
                if not position.dividends:
                    continue
                expiring_date = position.dividends.expiring_date
                if not expiring_date or expiring_date <= current_date:
                    position.dividends = None

    def _order_check(self, order):
        """
        Check if the order is a valid security order

        Args:
            order(PMSOrder): order
        """
        # assert portfolio can trade
        order._order_time = ' '.join([clock.current_date.strftime('%Y%m%d'), clock.current_minute])
        if order.symbol not in self.current_exchangeable:
            order._state = OrderState.REJECTED
            order._state_message = OrderStateMessage.NINC_HALT
        if order.order_amount == 0:
            order._state = OrderState.REJECTED
            order._state_message = OrderStateMessage.INVALID_AMOUNT
        if order.direction == -1:
            position_info = query_from_('redis', SchemaType.position,
                                        portfolio_id=order.portfolio_id).get(order.portfolio_id)
            available_amount = 0
            if position_info:
                current_position = position_info.positions.get(order.symbol)
                if current_position:
                    available_amount = current_position.available_amount
            if order.order_amount > available_amount:
                order._state = OrderState.REJECTED
                order._state_message = OrderStateMessage.NO_ENOUGH_AMOUNT
            if order.order_amount % 100 != 0:
                if (available_amount - order.order_amount) % 100 != 0:
                    order._state = OrderState.REJECTED
                    order._state_message = OrderStateMessage.INVALID_AMOUNT
                    msg = """order: {}, sell amount: {} is invalid, current available: {}!""".format(
                        order.order_id, str(order.order_amount), str(available_amount))
                    logger.debug('[SECURITY] [ACCEPT ORDERS FAILED]'+msg)
        else:
            if order.order_amount % 100 != 0:
                order._state = OrderState.REJECTED
                order._state_message = OrderStateMessage.INVALID_AMOUNT
                msg = """order: {}, buy amount: {} is invalid!""".format(order.order_id, str(order.order_amount))
                logger.debug('[SECURITY] [ACCEPT ORDERS FAILED]'+msg)

        msg = order.__repr__()
        logger.debug('[SECURITY] [ACCEPT ORDERS]'+msg)
        return order
