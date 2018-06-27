# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
import json
from lib.event.event_base import EventType
from lib.event.event_engine import EventEngine
from lib.gateway.ctpGateway.trader_gateway import CtpTraderGateway
from lib.gateway.strategy_gateway import StrategyGateway
from lib.trade.order import Order


data = json.load(open('CTP_connect.json', 'r+'))
address = data['tdAddress']
user_id = data['userID']
password = data['password']
broker_id = data['brokerID']
event_engine = EventEngine()
trader_gateway = CtpTraderGateway(user_id=user_id,
                                  password=password,
                                  broker_id=broker_id,
                                  address=address,
                                  event_engine=event_engine)
strategy_gateway = StrategyGateway()
event_engine.register_handlers(EventType.event_on_tick, getattr(strategy_gateway, EventType.event_on_tick))
event_engine.start()
trader_gateway.connect()
trader_gateway.query_account()
import time
time.sleep(1)
trader_gateway.query_positions()
# 开限价单成交
order = Order(symbol='i1809', order_amount=-5, offset_flag='open',
              price=483, order_type='limit', order_time='2018-05-16')
# order = Order(symbol='i1809', order_amount=-5, offset_flag='open', order_type='market', order_time='2018-05-17')
trader_gateway.send_order(order)
time.sleep(1)
trader_gateway.cancel_order('000001')
