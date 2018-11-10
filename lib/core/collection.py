# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
from utils.error import Errors
from . enums import SchemaType
from .. data.mongodb_base import *


class MongodbCollections(object):
    """
    Mongodb Collections
    """
    portfolio = mongodb_database['portfolio']
    order = mongodb_database['order']
    position = mongodb_database['position']
    trade = mongodb_database['trade']


class RedisCollections(object):

    order = 'order'
    position = 'position'
    trade = 'trade'


def switch_collection(database, schema_type):
    """
    Switch collection
    Args:
        database(string): database name
        schema_type(string): schema type

    Returns:
        collection(obj): collection
    """
    if database == 'mongodb':
        if schema_type == SchemaType.portfolio:
            collection = MongodbCollections.portfolio
        elif schema_type == SchemaType.order:
            collection = MongodbCollections.order
        elif schema_type == SchemaType.position:
            collection = MongodbCollections.position
        elif schema_type == SchemaType.trade:
            collection = MongodbCollections.trade
        else:
            raise Errors.INVALID_SCHEMA_TYPE
    elif database == 'redis':
        if schema_type == RedisCollections.order:
            collection = RedisCollections.order
        elif schema_type == RedisCollections.position:
            collection = RedisCollections.position
        elif schema_type == RedisCollections.trade:
            collection = RedisCollections.trade
        else:
            raise Errors.INVALID_SCHEMA_TYPE
    else:
        raise Errors.INVALID_DATABASE
    return collection


__all__ = [
    'MongodbCollections',
    'RedisCollections',
    'switch_collection'
]
