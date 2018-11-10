# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Redis api
# **********************************************************************************#
import json
import time
from datetime import datetime
from utils.error import Errors
from utils.dict import DefaultDict
from . redis_base import (
    redis_client,
    RedisCollection,
    redis_queue,
)
from .. core.enums import SecuritiesType
from .. core.schema import (
    SchemaType,
    OrderSchema,
    PositionSchema,
    TradeSchema
)


def redis_lock(name):
    try:
        result = redis_client.set(name, 0, ex=60, nx=True)
        if result:
            return True
        return False
    except:
        return False


def redis_unlock(name, timeout=None):
    try:
        if timeout:
            time.sleep(timeout)
        redis_client.delete(name)
    except:
        pass


def _query_from_(collection, portfolio_id=None):
    """
    Query collection data from database

    Args:
        collection(collection): mongodb collection
        portfolio_id(string or list of string): optional, portfolio_id

    Returns:
        dict: collection data
    """
    if portfolio_id is None:
        response = redis_client.hgetall(collection)
        response = {
            key: json.loads(value) for key, value in response.iteritems() if value is not None
        }
    elif isinstance(portfolio_id, (str, unicode)):
        value = redis_client.hget(collection, portfolio_id)
        if value:
            response = {
                portfolio_id: json.loads(value)
            }
        else:
            response = {}
    elif isinstance(portfolio_id, (list, tuple, set)):
        temp = redis_client.hmget(collection, *portfolio_id)
        response = {}
        for idx, key in enumerate(portfolio_id):
            if temp[idx]:
                response[key] = json.loads(temp[idx])
    else:
        raise Errors.INVALID_PORTFOLIO_ID

    return response


def _query_from_queue_(collection):
    """
    Query collection data from redis queue

    Args:
        collection(collection): mongodb collection

    Returns:
        dict: collection data
    """
    items = redis_queue.get_all(key=collection)
    return items


def _dump_to_(collection, mapping):
    """
    Query collection data from database

    Args:
        collection(collection): mongodb collection
        mapping(dict): data, composite dict
    Returns:
        list: collection data
    """
    for key, value in mapping.iteritems():
        mapping[key] = json.dumps(value)
    if mapping:
        redis_client.hmset(collection, mapping)


def query_from_redis(schema_type, portfolio_id=None, **kwargs):
    """
    Query data from redis

    Args:
        schema_type(string): schema type
        portfolio_id(None, str or list): None--> all;
    """
    normalize_object = kwargs.get('normalize_object', True)
    if schema_type == SchemaType.order:
        query_data = _query_from_(RedisCollection.order, portfolio_id)
        for key in query_data.keys():
            query_data[key] = OrderSchema.from_query(query_data[key], normalize_object=normalize_object)
        return query_data
    if schema_type == SchemaType.position:
        query_data = _query_from_(RedisCollection.position, portfolio_id)
        for key in query_data.keys():
            query_data[key] = PositionSchema.from_query(query_data[key], securities_type=SecuritiesType.futures)
        return query_data
    if schema_type == SchemaType.trade:
        query_data = _query_from_queue_(RedisCollection.trade)
        current_date = datetime.today().strftime('%Y%m%d')
        schema_items = DefaultDict({
            'portfolio_id': None,
            'date': current_date,
            'trades': list()
        })
        for item in query_data:
            portfolio_id = item['portfolio_id']
            schema_items[portfolio_id]['portfolio_id'] = portfolio_id
            schema_items[portfolio_id]['trades'].append(item)
        return {
            key: TradeSchema.from_query(schema) for key, schema in schema_items.iteritems()
        }
    raise Errors.INVALID_SCHEMA_TYPE


def dump_schema_to_redis(schema_type, schema, **kwargs):
    """
    Dump schema to redis

    Args:
        schema_type(string): schema type
        schema(schema or dict of schema): schema object
    """
    if schema_type == SchemaType.order:
        collection = RedisCollection.order
    elif schema_type == SchemaType.position:
        collection = RedisCollection.position
    else:
        raise Errors.INVALID_SCHEMA_TYPE
    to_dict = kwargs.get('to_dict', True)
    if isinstance(schema, dict):
        data = \
            {portfolio_id: curr_schema.to_redis_item(to_dict=to_dict) for portfolio_id, curr_schema in
             schema.iteritems()}
    else:
        data = {schema.portfolio_id: schema.to_redis_item(to_dict=to_dict)}
    _dump_to_(collection, data)


def delete_keys_redis(*delete_keys):
    """
    Delete one or more keys in redis

    Args:
        delete_keys(tuple): keys will be deleted in redis
    Returns:
        result(int): deleted amounts
    """
    retry_nums = 0

    while retry_nums < 3:
        result = redis_client.delete(*delete_keys)

        if result != len(delete_keys):
            retry_nums += 1
        else:
            return result

    not_deleted = set(delete_keys) & set(redis_client.keys())
    result = len(delete_keys) - len(not_deleted)

    return result


def delete_items_in_redis(schema_type, keys):
    """
    Delete items in redis

    Args:
        schema_type(string): schema type
        keys(list): keys
    """
    if schema_type == SchemaType.order:
        collection = RedisCollection.order
    elif schema_type == SchemaType.position:
        collection = RedisCollection.position
    else:
        raise Errors.INVALID_SCHEMA_TYPE
    redis_client.hdel(collection, *keys)
