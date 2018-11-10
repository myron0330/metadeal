# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Batch tools
# **********************************************************************************#
from utils.error import Errors
from . enums import SchemaType
from . collection import MongodbCollections
from .. data.mongodb_base import BatchTool


class MongodbBatchTools(object):
    """
    Mongodb Batch tools
    """
    portfolio = BatchTool(MongodbCollections.portfolio, buffer_size=2000)
    order = BatchTool(MongodbCollections.order, buffer_size=2000)
    position = BatchTool(MongodbCollections.position, buffer_size=2000)
    trade = BatchTool(MongodbCollections.trade, buffer_size=2000)


def switch_batch_tool(schema_type, database='mongodb'):
    """
    Switch batch tool
    Args:
        schema_type(string): schema type
        database(string): database name

    Returns:
        collection(obj): collection
    """
    if database == 'mongodb':
        if schema_type == SchemaType.portfolio:
            batch_tool = MongodbBatchTools.portfolio
        elif schema_type == SchemaType.order:
            batch_tool = MongodbBatchTools.order
        elif schema_type == SchemaType.position:
            batch_tool = MongodbBatchTools.position
        elif schema_type == SchemaType.trade:
            batch_tool = MongodbBatchTools.trade
        else:
            raise Errors.INVALID_SCHEMA_TYPE
    else:
        raise Errors.INVALID_DATABASE
    return batch_tool


__all__ = [
    'MongodbBatchTools',
    'switch_batch_tool'
]
