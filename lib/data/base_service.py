# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
from abc import ABCMeta, abstractmethod


class ServiceInterface(object):

    __metaclass__ = ABCMeta

    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    def batch_load_data(self, start, end, universe=None, **kwargs):
        """
        Batch load data.
        """
        raise NotImplementedError

    @abstractmethod
    def subset(self, start, end, universe=None, **kwargs):
        """
        Subset data.
        """
        raise NotImplementedError
