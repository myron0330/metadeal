# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
import json
from lib.gateway.ctp_gateway import CTPMarketGateway


data = json.load(open('CTP_connect.json', 'r+'))
market_gateway = CTPMarketGateway()
address = data['mdAddress']
user_id = data['userID']
password = data['password']
broker_id = data['brokerID']
address = str(address)
market_gateway.connect(user_id=user_id, password=password, broker_id=broker_id, address=address)
market_gateway.subscribe('rb1810')
