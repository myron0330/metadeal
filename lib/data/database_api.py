# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Database API.
#     Desc: define general database API for the service.
# **********************************************************************************#
from configs import api_token
from api_client import Client


client = Client()
client.init(api_token)


def load_daily_bar(start, end, symbols):
    """
    Load futures daily bar daa
    """
    pass
