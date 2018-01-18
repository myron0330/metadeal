# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Basic enumerates.
# **********************************************************************************#


class SchemaType(object):

    portfolio = 'portfolio'
    order = 'order'
    position = 'position'
    trade = 'trade'

    redis_available = [order, position]


class SecuritiesType(object):
    """
    Securities type
    """
    SECURITY = 'security'
    FUTURES = 'futures'

    ALL = [SECURITY, FUTURES]
