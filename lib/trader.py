# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Trader.
# **********************************************************************************#
from utils.decorator import singleton
from . configs import *

uwsgi_tag = False
try:
    from uwsgidecorators import thread, cron

    uwsgi_tag = True
except:
    from threading import Thread

    def thread(func):
        def decorator():
            t = Thread(target=func)
            t.start()
        return decorator

    def spool(func):
        def wrapper(*args, **kw):
            return func(*args, **kw)
        return wrapper

    def cron(a, b, c, d, e):
        def decorator(func):
            def wrapper(*args, **kw):
                return func(*args, **kw)
            return wrapper
        return decorator

    def timer(t):
        def decorator(func):
            def wrapper(*args, **kw):
                return func(*args, **kw)
            return wrapper
        return decorator


_parse_slots = (lambda x: (int(x.split(':')[0]), int(x.split(':')[1])))
futures_pre_slots = _parse_slots(futures_pre_trading_task_time)
futures_post_slots = _parse_slots(futures_post_trading_task_time)


@cron(futures_pre_slots[1], futures_pre_slots[0], -1, -1, -1)
def pre_trading_task():
    """
    Pre trading day task: including prepare history database, reload schema.
    """
    pass


@cron(futures_pre_slots[1], futures_pre_slots[0], -1, -1, -1)
def post_trading_task():
    """
    Post trading day task: including prepare history database, reload schema.
    """
    pass


@thread
def feedback_worker():
    """
    Trades worker: accept trades records from exchange.
    """
    while feedback_worker_enable:
        pass
    pass


@thread
def database_worker():
    """
    Database worker: synchronize valid database from redis to mongodb
    """
    while database_worker_enable:
        pass
    pass


@singleton
class Trader(object):

    def feed_orders(self, orders):
        pass

    def accept_trade(self):
        pass
