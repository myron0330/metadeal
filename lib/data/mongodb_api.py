from utils.error_utils import Errors
from . mongodb_base import *
from .. core.schema import (
    PortfolioSchema,
    OrderSchema,
    PositionSchema,
    TradeSchema
)
from .. core.enums import SecuritiesType, SchemaType


def _query_from_(collection, schema_cls, portfolio_id=None, date=None,
                 key='portfolio_id', **kwargs):
    """
    Query collection data from database

    Args:
        collection(collection): mongodb collection
        schema_cls(schema): schema class
        portfolio_id(string): optional, portfolio_id
        date(string): optional, query date
        key(string): optional, key

    Returns:
        list: collection data
    """
    args, result = dict(), dict()
    if portfolio_id:
        args['portfolio_id'] = portfolio_id
    if date:
        args['date'] = date
    args.update(kwargs)
    response = collection.find(args)
    if key is None:
        result = list()
        for query_data in response:
            query_data.pop('_id')
            result.append(schema_cls.from_query(query_data))
    else:
        for query_data in response:
            query_data.pop('_id')
            result[query_data[key]] = schema_cls.from_query(query_data)
    return result


def _query_from_by(collection, schema_cls, portfolio_id=None, date=None,
                   key='portfolio_id', securities_type=SecuritiesType.SECURITY, **kwargs):
    """
    Query collection data from database by securities type

    Args:
        collection(collection): mongodb collection
        schema_cls(schema): schema class
        portfolio_id(string): optional, portfolio_id
        date(string): optional, query date
        key(string): optional, key
        securities_type(string): securities type

    Returns:
        list: collection data
    """
    args, result = dict(), dict()
    if portfolio_id:
        args['portfolio_id'] = portfolio_id
    if date:
        args['date'] = date
    args.update(kwargs)
    response = collection.find(args)
    if key is None:
        result = list()
        for query_data in response:
            query_data.pop('_id')
            result.append(schema_cls.from_query(query_data, securities_type))
    else:
        for query_data in response:
            query_data.pop('_id')
            result[query_data[key]] = schema_cls.from_query(query_data, securities_type)
    return result


def query_from_mongodb(schema_type, portfolio_id=None, date=None, key='portfolio_id', **kwargs):
    """
    Query schema from mongodb

    Args:
        schema_type(string): schema type
        portfolio_id(string): optional, portfolio id
        date(string): optional, query date
        key(string): optional, key
    """
    if schema_type == SchemaType.order:
        return _query_from_(order_collection, OrderSchema,
                            portfolio_id=portfolio_id, date=date, key=key, **kwargs)
    if schema_type == SchemaType.position:
        return _query_from_by(position_collection, PositionSchema, portfolio_id=portfolio_id,
                              date=date, key=key, securities_type=SecuritiesType.SECURITY, **kwargs)
    if schema_type == SchemaType.futures_position:
        return _query_from_by(position_collection, PositionSchema, portfolio_id=portfolio_id,
                              date=date, key=key, securities_type=SecuritiesType.FUTURES, **kwargs)
    if schema_type == SchemaType.portfolio:
        return _query_from_(portfolio_collection, PortfolioSchema,
                            portfolio_id=portfolio_id, date=date, key=key, **kwargs)
    if schema_type == SchemaType.trade:
        return _query_from_(trade_collection, TradeSchema,
                            portfolio_id=portfolio_id, date=date, key=key, **kwargs)
    raise Errors.INVALID_SCHEMA_TYPE


def dump_schema_to_mongodb(schema_type, schema, unit_dump=True):
    """
    Dump schema to mongodb

    Args:
        schema_type(string): schema type
        schema(schema or list, dict of schema): schema object
        unit_dump(boolean): whether to do unit dump
    """
    if schema_type == SchemaType.order:
        batch_op = order_batch_op
    elif schema_type == SchemaType.position:
        batch_op = position_batch_op
    elif schema_type == SchemaType.portfolio:
        batch_op = portfolio_batch_op
    elif schema_type == SchemaType.trade:
        batch_op = trade_batch_op
    else:
        raise Errors.INVALID_SCHEMA_TYPE
    if isinstance(schema, list):
        for _ in schema:
            batch_op.append(*_.to_mongodb_item())
    else:
        schema_iter = schema if isinstance(schema, dict) else {schema.portfolio_id: schema}
        for _, schema in schema_iter.iteritems():
            batch_op.append(*schema.to_mongodb_item())
    if unit_dump:
        batch_op.commit()
