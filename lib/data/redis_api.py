# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Redis api
# **********************************************************************************#
import json
import time
from utils.error_utils import Errors
from . redis_base import redis_client, redis_queue
from .. core.collection import switch_collection


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
    finally:
        pass


def _query_from_(collection, schema_id=None):
    """
    Query collection data from database

    Args:
        collection(collection): mongodb collection
        schema_id(string or list of string): optional, portfolio_id

    Returns:
        dict: collection data
    """
    if schema_id is None:
        response = redis_client.hgetall(collection)
    elif isinstance(schema_id, (str, unicode)):
        response = {
            schema_id: redis_client.hget(collection, schema_id)
        }
    elif isinstance(schema_id, (list, tuple, set)):
        response = dict(zip(schema_id, redis_client.hmget(collection, *schema_id)))
    else:
        raise Errors.INVALID_SCHEMA_ID_INPUT
    response = {
        key: json.loads(value) for key, value in response.iteritems() if value is not None
    }
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


def query_from_redis(schema_type, schema_id=None):
    """
    Query data from redis

    Args:
        schema_type(string): schema type
        schema_id(None, str or list): None--> all;
    """
    collection = switch_collection('redis', schema_type)
    query_data = _query_from_(collection, schema_id)
    # todo. deal with query data.
    return query_data


def dump_schema_to_redis(schema_type, schema):
    """
    Dump schema to redis

    Args:
        schema_type(string): schema type
        schema(schema or dict of schema): schema object
    """
    collection = switch_collection('redis', schema_type)
    if isinstance(schema, dict):
        data = \
            {schema_id: curr_schema.to_redis_item() for schema_id, curr_schema in schema.iteritems()}
    else:
        data = {schema.portfolio_id: schema.to_redis_item()}
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
    result = len(delete_keys)-len(not_deleted)
    return result


def delete_items_in_redis(schema_type, keys):
    """
    Delete items in redis

    Args:
        schema_type(string): schema type
        keys(list): keys
    """
    collection = switch_collection('redis', chema_type)
    redis_client.hdel(collection, *keys)
