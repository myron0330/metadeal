# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Decorator utils
# **********************************************************************************#
from threading import Lock


def mutex_lock(func, lock=Lock()):

    def decorator(*args, **kwargs):
        try:
            lock.acquire()
            result = func(*args, **kwargs)
        finally:
            lock.release()
        return result

    return decorator

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
