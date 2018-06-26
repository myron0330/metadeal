# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: PMS broker: broker pms_agent for brokers in PMS.
# **********************************************************************************#
from utils.dict_utils import DefaultDict
from utils.error_utils import Errors
from . base import *
from . pms_agent.futures_pms_agent import FuturesPMSAgent
from .. configs import logger
from .. core.enums import SecuritiesType
from .. core.schema import SchemaType
from .. database.database_api import query_from_


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

    def pre_trading_day(self, securities_type=SecuritiesType.ALL, force_date=None,
                        portfolio_ids=None):
        """
        Pre trading day: tasks before market opening

        Args:
            securities_type(string): securities type
            force_date(datetime.datetime): specific a base date
            portfolio_ids(list): portfolio ID list
        """
        securities_type_list = list_wrap_(securities_type)
        for securities_type in securities_type_list:
            if securities_type == SecuritiesType.futures:
                self.pms_lites[securities_type].pre_trading_day(force_date=force_date, portfolio_ids=portfolio_ids)

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

    @staticmethod
    def cancel_orders(to_cancel_list=None):
        """
        Cancel orders by portfolio_id.

        Args:
            to_cancel_list: cancel order list
        """
        import pandas as pd
        from lib.trade.order import OrderState, OrderStateMessage
        to_cancel_frame = pd.DataFrame(to_cancel_list).drop_duplicates()
        if to_cancel_frame.empty:
            return
        group_result = to_cancel_frame.groupby('portfolio_id')
        order_schemas = query_from_('redis', SchemaType.order, portfolio_id=list(group_result.groups))
        for portfolio_id, order_schema in order_schemas.iteritems():
            frame = group_result.get_group(portfolio_id)
            for order_id in frame.order_id:
                order = order_schema.orders.get(order_id)
                if not order:
                    continue
                if order.state in OrderState.ACTIVE:
                    order._state = OrderState.CANCELED
                    order._state_message = OrderStateMessage.CANCELED
        dump_to_('redis', SchemaType.order, order_schemas)

    def post_trading_day(self, securities_type=SecuritiesType.ALL,
                         force_date=None,
                         portfolio_ids=None):
        """
        Post trading day: tasks after market trading

        Args:
            securities_type(string): securities type
            force_date(datetime.datetime): specific a base date
            portfolio_ids(list): portfolio id list
        """
        securities_type_list = list_wrap_(securities_type)
        for securities_type in securities_type_list:
            if securities_type == SecuritiesType.futures:
                self.pms_lites[securities_type].post_trading_day(force_date=force_date, portfolio_ids=portfolio_ids)

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
        futures_portfolio_info = {portfolio_id: schema for portfolio_id, schema in portfolio_info.iteritems()
                                  if schema.account_type == SecuritiesType.futures}
        position_info = dict()
        if futures_portfolio_info:
            position_info.update(
                query_from_('redis', SchemaType.position, portfolio_id=futures_portfolio_info.keys()))
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
            if securities_type == SecuritiesType.futures:
                position_info_dict[SecuritiesType.futures][portfolio_id] = position_schema
            else:
                message = '[EVALUATE] portfolio_id: {}, invalid schema type.'.format(portfolio_id)
                logger.debug(message)
                continue
            if with_benchmark:
                if securities_type == SecuritiesType.futures:
                    benchmark_info_dict[SecuritiesType.futures][portfolio_id] = portfolio_info[portfolio_id].benchmark
                else:
                    message = '[EVALUATE] portfolio_id: {}, invalid schema type.'.format(portfolio_id)
                    logger.debug(message)
                    continue
        for securities_type, info in position_info_dict.iteritems():
            if securities_type == SecuritiesType.futures:
                current_pms_agent = self.futures_pms_agent
            else:
                raise Errors.INVALID_SECURITIES_TYPE
            evaluated_info = current_pms_agent.evaluate(position_info=info,
                                                        benchmark_dict=benchmark_info_dict[securities_type],
                                                        force_evaluate_date=force_evaluate_date)
            evaluated_position_info.update(evaluated_info)
        return evaluated_position_info
