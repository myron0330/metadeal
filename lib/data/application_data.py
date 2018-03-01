# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
from . base_service import ServiceInterface


class ApplicationData(ServiceInterface):
    """
    External application data.
    """
    def __init__(self):
        super(ApplicationData, self).__init__()

    def batch_load_data(self, start, end, universe=None, **kwargs):
        pass

    def subset(self, start, end, universe=None, **kwargs):
        return self
