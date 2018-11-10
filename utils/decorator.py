# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Decorator utils
# **********************************************************************************#
import os
import time
from psutil import Process
from threading import Thread, Lock
from datetime import datetime
from collections import OrderedDict
from functools import wraps


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

    @wraps(cls)
    def decorator(*args, **kwargs):
        if cls not in _instance:
            _instance[cls] = cls(*args, **kwargs)
        return _instance[cls]

    return decorator


def memory_monitor(slot=0.1):
    """
    Decorator: memory monitor.

    Args:
        slot(float): sampling frequency.

    Returns:
        func, memory: function output and memory usage.
    """
    def _get_memory(monitor):
        """
        Get memory usage.
        """
        info = monitor.memory_full_info()
        return info.uss / 1024. / 1024.

    class _MonitorEngine(object):
        """
        Monitor engine.
        """
        def __init__(self, container=None, monitor=None, active=False, timeout=0.5):
            self._container = container
            self._monitor = monitor
            self._active = active
            self._thread = Thread(target=self._run, kwargs={'timeout': timeout})

        @property
        def container(self):
            """
            Container.
            """
            return self._container

        def _run(self, timeout=1):
            """
            Running function.
            """
            while self._active:
                current_time = str(datetime.now())
                self._container[current_time] = _get_memory(self._monitor)
                time.sleep(timeout)

        def start(self):
            """
            Start.
            """
            self._active = True
            self._thread.start()

        def stop(self):
            """
            Stop.
            """
            self._active = False
            self._thread.join()

    monitor_engine = _MonitorEngine(container=OrderedDict(),
                                    monitor=Process(os.getpid()),
                                    timeout=slot)

    def decorator(func):
        """
        Outer Decorator.
        """
        def _decorator(*args, **kwargs):
            """
            Inner decorator.
            """
            monitor_engine.start()
            result = func(*args, **kwargs)
            monitor_engine.stop()
            return result, monitor_engine.container

        return _decorator

    return decorator
