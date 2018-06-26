# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
import numpy as np
from utils.dict_utils import DefaultDict
from utils.error_utils import Errors
from .. core.schema import *
from .. core.clock import clock
from .. core.enums import (
    SecuritiesType,
    SchemaType
)
from .. core.collection import MongodbCollections
from .. database.database_api import (
    query_from_,
    dump_to_,
    delete_,
    delete_items_
)
from .. trade.order import BaseOrder


def update_(database, schema_type, items, date=None, **kwargs):
    """
    Update items to redis

    Args:
        database(string): database name
        schema_type(string): schema type
        items(list or dict): trade items
        date(datetime.datetime): datetime
    """
    current_date = (date or clock.current_date).strftime('%Y%m%d')
    if database == 'redis' and schema_type not in SchemaType.redis_available:
        raise Errors.INVALID_REDIS_SCHEMA_TYPE
    if schema_type == SchemaType.order:
        order_info = DefaultDict(OrderSchema(date=clock.current_date.strftime('%Y%m%d')))
        original_order_info = \
            kwargs.get('original_order_info', query_from_(database, SchemaType.order,
                                                          portfolio_id=list(set(item.portfolio_id for item in items)),
                                                          normalize_object=False))
        order_info.update(original_order_info)
        for order in items:
            portfolio_id = order.portfolio_id
            order_schema = order_info[portfolio_id]
            order_schema.portfolio_id = portfolio_id
            order_schema.date = current_date
            order_schema.orders.update({order.order_id: order.to_dict()})
        dump_to_(database, SchemaType.order, order_info, to_dict=False)
    if schema_type == SchemaType.trade:
        trade_info = DefaultDict(TradeSchema(date=clock.current_date.strftime('%Y%m%d')))
        for trade in items:
            portfolio_id = trade.portfolio_id
            trade_schema = trade_info[portfolio_id]
            trade_schema.date = current_date
            trade_schema.portfolio_id = portfolio_id
            trade_schema.trades.append(trade)
        dump_to_(database, SchemaType.trade, trade_info)


def query_portfolio_info_by_(securities_type=SecuritiesType.futures):
    """
    Query portfolio info by securities type

    Args:
        securities_type(string): securities type
    """
    info = query_from_('mongodb', SchemaType.portfolio,
                       account_type=securities_type,
                       delete_flag=False)
    return info


def query_portfolio_ids_by_(securities_type=SecuritiesType.futures):
    """
    Query portfolio ids by securities type

    Args:
        securities_type(string): securities type
    """
    result = list(
        MongodbCollections.portfolio.find({'account_type': securities_type, 'delete_flag': False}, {'portfolio_id': 1}))
    portfolio_ids = [item['portfolio_id'] for item in result]
    return portfolio_ids


def query_by_ids_(database, schema_type, date, portfolio_ids):
    """
    Query items by ids

    Args:
        database(string): database name
        schema_type(string): schema type
        date(datetime.datetime): datetime
        portfolio_ids(list): portfolio ids
    """
    if date:
        date = date.strftime('%Y%m%d')
    if database == 'mongodb':
        portfolio_query_fields = {'$in': portfolio_ids}
        schema_list = query_from_(database, schema_type,
                                  date=date,
                                  portfolio_id=portfolio_query_fields,
                                  key=None)
        schema_info = {schema.portfolio_id: schema for schema in schema_list}
    elif database == 'redis':
        schema_info = query_from_(database, schema_type, portfolio_id=portfolio_ids)
    else:
        raise Errors.INVALID_DATABASE
    return schema_info


def query_by_securities_(database, schema_type, date, securities_type=SecuritiesType.futures):
    """
    Query items by ids

    Args:
        database(string): database name
        schema_type(string): schema type
        date(datetime.datetime): datetime
        securities_type(string): securities type
    """
    portfolio_ids = query_portfolio_info_by_(securities_type).keys()
    return query_by_ids_(database, schema_type, date, portfolio_ids)


def delete_redis_(schema_types, items):
    """
    Delete redis items

    Args:
        schema_types(list): schema types
        items(list): items
    """
    for schema_type in schema_types:
        delete_items_('redis', schema_type, items=items)


def calc_return(pre_value, current_value):
    """
    Calculate return.

    Args:
        pre_value(float): pre value
        current_value(float): current value
    """
    result = current_value / pre_value - 1
    if np.isnan(result) or np.isinf(result):
        result = 0.
    return result


def list_wrap_(obj):
    """
    normalize input to list

    Args:
        obj(obj): input values
    """
    return list(obj) if isinstance(obj, (list, tuple)) else [obj]


def change_order_state(order, target_state=None, target_message=None):
    """
    Change order state.

    Args:
        order(PMSOrder): order object
        target_state(string): target order state
        target_message(string): target order state message

    """
    if order is None:
        return
    if not isinstance(order, BaseOrder):
        raise Errors.INVALID_ORDER_OBJECT
    if target_state is not None:
        order.state = target_state
    if target_message is not None:
        order.state_message = target_message


__all__ = [
    'query_from_',
    'dump_to_',
    'update_',
    'delete_',
    'query_portfolio_info_by_',
    'query_portfolio_ids_by_',
    'query_by_ids_',
    'query_by_securities_',
    'delete_redis_',
    'calc_return',
    'list_wrap_',
    'change_order_state'
]
