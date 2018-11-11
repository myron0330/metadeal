"""
# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
#   Author: Myron
# **********************************************************************************#
"""
from .. core.enums import BaseEnums


class SecuritiesType(BaseEnums):
    """
    Securities type
    """
    futures = 'futures'
    digital_currency = 'digital_currency'

    ALL = [futures, digital_currency]
