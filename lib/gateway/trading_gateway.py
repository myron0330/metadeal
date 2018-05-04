# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
from . base_gateway import BaseTradingGateway


class TradingGateway(BaseTradingGateway):

    def start(self):
        """
        Start
        """
        raise NotImplementedError

    def stop(self):
        """
        Stop
        """
        raise NotImplementedError

    def on_bar(self, *args, **kwargs):
        """
        On bar response
        """
        raise NotImplementedError

    def on_portfolio(self, *args, **kwargs):
        """
        On portfolio response
        """
        raise NotImplementedError
