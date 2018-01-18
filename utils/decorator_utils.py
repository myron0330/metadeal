# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Decorator utils
# **********************************************************************************#


def singleton(cls):
    """
    Class Decorator: single instance.

    Args:
        cls(obj): class
    """
    _instance = dict()

    def decorator(*args, **kwargs):
        if cls not in _instance:
            _instance[cls] = cls(*args, **kwargs)
        return _instance[cls]

    return decorator