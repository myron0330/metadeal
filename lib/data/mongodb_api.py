# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Mongodb api
# **********************************************************************************#
from .. core.schema import switch_schema
from .. core.collection import switch_collection
from .. core.batch_tool import switch_batch_tool


def _query_from_(collection, schema_cls, portfolio_id=None, date=None,
                 key='portfolio_id', query_parameters=None, **kwargs):
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
    query_parameters = query_parameters or dict()
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
            result.append(schema_cls.from_query(query_data, **query_parameters))
    else:
        for query_data in response:
            query_data.pop('_id')
            result[query_data[key]] = schema_cls.from_query(query_data, **query_parameters)
    return result


def query_from_mongodb(schema_type, portfolio_id=None, date=None,
                       key='portfolio_id', query_parameters=None, **kwargs):
    """
    Query schema from mongodb

    Args:
        schema_type(string): schema type
        portfolio_id(string): optional, portfolio id
        date(string): optional, query date
        key(string): optional, key
        query_parameters(dict): query parameters
    """
    collection = switch_collection('mongodb', schema_type)
    schema = switch_schema(schema_type)
    schemas = _query_from_(collection, schema,
                           portfolio_id=portfolio_id,
                           date=date, key=key,
                           query_parameters=query_parameters,
                           **kwargs)
    return schemas


def dump_schema_to_mongodb(schema_type, schema, unit_dump=True):
    """
    Dump schema to mongodb

    Args:
        schema_type(string): schema type
        schema(schema or list, dict of schema): schema object
        unit_dump(boolean): whether to do unit dump
    """
    batch_tool = switch_batch_tool(schema_type)
    if isinstance(schema, list):
        for _ in schema:
            batch_tool.append(*_.to_mongodb_item())
    else:
        schema_iter = schema if isinstance(schema, dict) else {schema.portfolio_id: schema}
        for _, schema in schema_iter.iteritems():
            batch_tool.append(*schema.to_mongodb_item())
    if unit_dump:
        batch_tool.commit()
