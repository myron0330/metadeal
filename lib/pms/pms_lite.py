# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: PMS broker: broker pms_agent for brokers in PMS.
# **********************************************************************************#
from lib.context.clock import clock
from .base import *
from .pms_agent.futures_pms_agent import FuturesPMSAgent
from .pms_agent.security_pms_agent import SecurityPMSAgent
from .pms_broker import PMSBroker
from .. import logger
from ..core.enum import SecuritiesType
from ..core.schema import SchemaType
from ..database.database_api import load_adjust_close_price, query_from_
from ..market.market_quote import MarketQuote
from ..utils.date_utils import get_previous_trading_date
from ..utils.dict_utils import DefaultDict
from ..utils.error_utils import Errors


class PMSLiteMcs(type):
    """
    PMSLite meta class
    """
    def __new__(mcs, name, bases, attributes):
        pms_lites = {key: value for key, value in attributes.iteritems() if key.endswith('_pms_agent')}
        attributes['pms_lites'] = {name.replace('_pms_agent', ''): attributes[name] for name in pms_lites}
        return type.__new__(mcs, name, bases, attributes)


class PMSLite(object):
    """
    PMSLite: pms lite agent for managing all pms_lites in PMS, add tail_fix '_pms_lite' if expand other pms lite.
    """
    __metaclass__ = PMSLiteMcs

    pms_broker = PMSBroker()
    security_pms_agent = SecurityPMSAgent()
    futures_pms_agent = FuturesPMSAgent()

    def __new__(cls, *args, **kwargs):
        """
        Single instance pms broker
        """
        if not hasattr(cls, '_instance'):
            cls._instance = super(PMSLite, cls).__new__(cls)
        return cls._instance

    def prepare(self, securities_type=SecuritiesType.ALL):
        """
        Prepare when service is loading

        Args:
            securities_type(string or list): securities type
        """
        securities_type_list = list_wrap_(securities_type)
        for securities_type in securities_type_list:
            self.pms_lites[securities_type].prepare()

    def pre_trading_day(self, securities_type=SecuritiesType.ALL,
                        with_dividend=True, force_date=None):
        """
        Pre trading day: tasks before market opening

        Args:
            securities_type(string): securities type
            with_dividend(boolean): if dividends is needed
            force_date(datetime.datetime): specific a base date
        """
        securities_type_list = list_wrap_(securities_type)
        for securities_type in securities_type_list:
            if securities_type == SecuritiesType.SECURITY:
                self.pms_lites[securities_type].pre_trading_day(with_dividend=with_dividend,
                                                                force_date=force_date)
            else:
                self.pms_lites[securities_type].pre_trading_day(force_date=force_date)

    def accept_orders(self, orders, securities_type=SecuritiesType.ALL):
        """
        Interface to accept orders from outside. Things to be done: 1) do orders pre_check;
                                                                    2) dump orders to database;
                                                                    3) send valid orders to broker for transact;
        Args:
            orders(list): orders requests
            securities_type(string): securities type
        """
        self.pms_lites[securities_type].accept_orders(orders)

    def post_trading_day(self, securities_type=SecuritiesType.ALL,
                         with_dividend=True, force_date=None):
        """
        Post trading day: tasks after market trading

        Args:
            securities_type(string): securities type
            with_dividend(boolean): if dividends is needed
            force_date(datetime.datetime): specific a base date
        """
        securities_type_list = list_wrap_(securities_type)
        for securities_type in securities_type_list:
            if securities_type == SecuritiesType.SECURITY:
                self.pms_lites[securities_type].post_trading_day(with_dividend=with_dividend,
                                                                 force_date=force_date)
            else:
                self.pms_lites[securities_type].post_trading_day(force_date=force_date)

    def clear(self, securities_type=SecuritiesType.ALL):
        """
        Clear info

        Args:
            securities_type(string): securities type
        """
        securities_type_list = list_wrap_(securities_type)
        for securities_type in securities_type_list:
            self.pms_lites[securities_type].clear()

    def evaluated_positions_by_(self, portfolio_ids, with_benchmark=False, portfolio_info=None):
        """
        Evaluated position info

        Args:
            portfolio_ids(list): portfolio ids
            with_benchmark(boolean): if benchmark evaluation is needed
            portfolio_info(dict): portfolio_info
        """
        if not portfolio_info:
            mongodb_query_fields = {'$in': portfolio_ids}
            portfolio_info = query_from_('mongodb', SchemaType.portfolio, portfolio_id=mongodb_query_fields)
        securities_portfolio_info = {portfolio_id: schema for portfolio_id, schema in portfolio_info.iteritems()
                                     if schema.account_type == SecuritiesType.SECURITY}
        futures_portfolio_info = {portfolio_id: schema for portfolio_id, schema in portfolio_info.iteritems()
                                  if schema.account_type == SecuritiesType.FUTURES}
        position_info = dict()
        if securities_portfolio_info:
            position_info.update(
                query_from_('redis', SchemaType.position, portfolio_id=securities_portfolio_info.keys()))
        if futures_portfolio_info:
            position_info.update(
                query_from_('redis', SchemaType.futures_position, portfolio_id=futures_portfolio_info.keys()))
        position_info = self.evaluate(portfolio_info=portfolio_info,
                                      position_info=position_info,
                                      with_benchmark=with_benchmark)
        return position_info

    def evaluate(self, portfolio_info=None,
                 position_info=None,
                 force_evaluate_date=None,
                 with_benchmark=False):
        """
        计算持仓市值、浮动盈亏以及当日用户权益
        Args:
            portfolio_info(obj or dict): portfolio schema dict
            position_info(obj or dict): position schema dict
            force_evaluate_date(datetime.datetime): assign a specific date
            with_benchmark(boolean): if benchmark evaluation is needed

        Returns:
            dict: evaluated position schema dict
        """
        portfolio_info = \
            portfolio_info if isinstance(portfolio_info, dict) else {portfolio_info.portfolio_id: portfolio_info}
        position_info = \
            position_info if isinstance(position_info, dict) else {position_info.portfolio_id: position_info}
        evaluated_position_info = dict()
        position_info_dict = DefaultDict(dict)
        benchmark_info_dict = DefaultDict(dict)

        for portfolio_id, position_schema in position_info.iteritems():
            if portfolio_info.get(portfolio_id) is None:
                message = '[EVALUATE] portfolio_id: {}, no record in database.'.format(portfolio_id)
                logger.debug(message)
                continue
            securities_type = portfolio_info[portfolio_id].account_type
            if securities_type == SecuritiesType.SECURITY:
                position_info_dict[SecuritiesType.SECURITY][portfolio_id] = position_schema
            elif securities_type == SecuritiesType.FUTURES:
                position_info_dict[SecuritiesType.FUTURES][portfolio_id] = position_schema
            else:
                message = '[EVALUATE] portfolio_id: {}, invalid schema type.'.format(portfolio_id)
                logger.debug(message)
                continue
            if with_benchmark:
                if securities_type == SecuritiesType.SECURITY:
                    benchmark_info_dict[SecuritiesType.SECURITY][portfolio_id] = portfolio_info[portfolio_id].benchmark
                elif securities_type == SecuritiesType.FUTURES:
                    benchmark_info_dict[SecuritiesType.FUTURES][portfolio_id] = portfolio_info[portfolio_id].benchmark
                else:
                    message = '[EVALUATE] portfolio_id: {}, invalid schema type.'.format(portfolio_id)
                    logger.debug(message)
                    continue
        for securities_type, info in position_info_dict.iteritems():
            if securities_type == SecuritiesType.SECURITY:
                current_pms_agent = self.security_pms_agent
            elif securities_type == SecuritiesType.FUTURES:
                current_pms_agent = self.futures_pms_agent
            else:
                raise Errors.INVALID_SECURITIES_TYPE
            evaluated_info = current_pms_agent.evaluate(position_info=info,
                                                        benchmark_dict=benchmark_info_dict[securities_type],
                                                        force_evaluate_date=force_evaluate_date)
            evaluated_position_info.update(evaluated_info)
        return evaluated_position_info

    def get_benchmark_interval_return(self, benchmark, start=None, end=None):
        """
        返回任意日期区间的指数累积收益
        Args:
            benchmark(string): benchmark symbol
            start(datetime.datetime): start date
            end(datetime.datetime): end date

        Returns:
            float: benchmark return
        """
        end = end or clock.current_date
        if end == clock.current_date:
            market_quote = MarketQuote.get_instance()
            last_price_info = market_quote.get_price_info(universes=[benchmark])
            end_price_item = last_price_info.get(benchmark)
        else:
            # todo. support futures as benchmark.
            end_price_item = {'closePrice': float(load_adjust_close_price(end, [benchmark]))}
        start = start or clock.current_date
        if start == clock.current_date:
            start_price = self.pms_broker.get_pre_close_price_of_(benchmark)
        else:
            # todo. support futures as benchmark.
            start = get_previous_trading_date(start)
            start_price = float(load_adjust_close_price(start, [benchmark]))
        if end_price_item and start_price is not None:
            cumulative_return = calc_return(start_price, end_price_item['closePrice'])
            return cumulative_return

    def get_current_benchmark_return(self, benchmark, base):
        """
        返回benchmark当天回报率，以及基于base值的累积回报率
        Args:
            benchmark(string): benchmark symbol
            base(float): benchmark base

        Returns:
            float: cumulative returns
        """
        market_quote = MarketQuote.get_instance()
        last_price_info = market_quote.get_price_info(universes=[benchmark])
        end_price_item = last_price_info.get(benchmark)
        start_price = self.pms_broker.get_pre_close_price_of_(benchmark)
        if end_price_item and start_price is not None:
            bk_return = calc_return(start_price, end_price_item['closePrice'])
            bk_cumulative_return = calc_return(base, end_price_item['closePrice'])
        else:
            bk_return = bk_cumulative_return = None
        return bk_return, bk_cumulative_return
