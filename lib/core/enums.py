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
    futures = 'futures'
    digital_currency = 'digital_currency'

    ALL = [futures, digital_currency]
