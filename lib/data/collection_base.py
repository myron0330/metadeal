# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
from mongodb_base import *


class MongodbCollections(object):
    """
    Mongodb Collections
    """
    portfolio = mongodb_database['portfolio']
    order = mongodb_database['order']
    position = mongodb_database['position']
    trade = mongodb_database['trade']


class MongodbBatchTools(object):
    """
    Mongodb Batch tools
    """
    portfolio = BatchTool(MongodbCollections.portfolio, buffer_size=2000)
    order = BatchTool(MongodbCollections.order, buffer_size=2000)
    position = BatchTool(MongodbCollections.position, buffer_size=2000)
    trade = BatchTool(MongodbCollections.trade, buffer_size=2000)


class RedisCollections(object):

    pass


def _switch_collections(database, schema_type):
    """
    Switch collections
    Args:
        database(string): database name
        schema_type(string): schema type

    Returns:
        collection(obj): collection
    """
    if database == 'mongodb':
        raise NotImplementedError
    elif database == 'redis':
        raise NotImplementedError
    raise NotImplementedError


__all__ = [
    'MongodbCollections',
    'MongodbBatchTools'
]
