# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Test demo strategy.
# **********************************************************************************#
from unittest import TestCase
from lib.trading import trading
from lib.core.clock import clock

DEFAULT_STRATEGY = """
universe = ['RBM0', 'RB1810']  # 希望订阅的货币对行情数据
freq = 'm'
refresh_rate = 1                           # 执行handle_data的时间间隔

accounts = {
    'account1': AccountConfig(account_type='futures'),
}

def initialize(context):                   # 初始化策略运行环境
    context.counter = 0


def handle_data(context):                  # 核心策略逻辑
    print '{} handle_data {}'.format('#' * 25, '#' * 25)
    print context.previous_date
    print context.current_date
    print context.current_minute
    print context.now
    account = context.get_account('account1')
    print account.get_positions()
    print account.get_position('BTC')
    print account.get_position('TEST')
    if context.counter == 0:
        order_id = account.order('RB1810', amount=0.01, order_type='limit', price=0.1)
        print 'order_id: {}'.format(order_id)
        context.counter += 1
    # account.cancel_order(order_id)
    # print 'cancel_order: {}'.format(order_id)
    print account.get_orders()
    print context.get_universe()
    print '\\n'

def on_tick(context, tick):
    print '{} on_tick {}'.format('#' * 25, '#' * 25)
    print tick
    account = context.get_account('account1')
    if context.counter == 1:
        order_id = account.order('RB1810', amount=-0.01, order_type='limit', price=0.1)
        print 'order_id: {}'.format(order_id)
        context.counter += 1
    print '\\n'
"""


class TestDemo(TestCase):

    def setUp(self):
        self.strategy_code = DEFAULT_STRATEGY
        self.connect_json = 'CTP_connect.json'

    def test_trading(self):
        trading(strategy_code=self.strategy_code, connect_json=self.connect_json)

    def test_clock(self):
        print clock.now
