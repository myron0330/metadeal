# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
import time
import traceback
from threading import Thread
from configs import logger
from subscriber_base import (
    fetch_order_from_pms,
    fetch_trade_from_pms
)
from quartz.event.event_base import EventType


class TradesSubscriber(object):

    def __init__(self, account_id, event_engine=None):
        self.account_id = account_id
        self.event_engine = event_engine
        self.order_list = list()
        self.order_status = ['PENDING_NEW', 'NEW', 'PARTIALLY_FILLED']
        self.trade_map = {}
        self._check_order_thread = Thread(target=self.check_order).start()
        self._fetch_trade_thread = Thread(target=self.fetch_trade).start()

    def put_order(self, order_id):
        """
        Put order Id.

        Args:
            order_id(string): order id
        """
        self.order_list.append(order_id)

    def fetch_trade(self):
        """
        Fetch trade record
        """
        while True:
            for order_id in self.order_list:
                try:
                    status_code, trade_list = self._fetch_trade_data(order_id)
                    if not str(status_code).startswith('20'):
                        continue
                except:
                    logger.error(traceback.format_exc())
                else:
                    if order_id in self.trade_map.keys():
                        increment_trade_list = self.sub_trade_list(trade_list, self.trade_map[order_id])
                    else:
                        increment_trade_list = trade_list
                    if not increment_trade_list:
                        continue

                    self.trade_map[order_id] = trade_list
                    if increment_trade_list:
                        self.event_engine.publish(EventType.event_subscribe_trade, trade_list=increment_trade_list)
            time.sleep(0.05)

    def check_order(self):
        """
        check order status, maintain order list
        :return:
        """
        while True:
            for order_id in self.order_list:
                try:
                    status_code, order = self._fetch_order_data(order_id)
                    if not str(status_code).startswith('20'):
                        continue
                    self.event_engine.publish(EventType.event_deal_with_order, order_data=order)
                except:
                    logger.error(traceback.format_exc())
                else:
                    if not order['orderStatus'] in self.order_status:
                        self.order_list.remove(order_id)
            time.sleep(0.5)

    @staticmethod
    def sub_trade_list(new_list, old_list):
        """
        获取交易记录的增量
        :param new_list: 新的交易记录列表
        :param old_list: 旧的交易记录列表
        :return: list, 增加的交易记录
        """
        increment_list = []
        trade_id_list = [trade['tradeId'] for trade in old_list]
        for trade in new_list:
            if not trade['tradeId'] in trade_id_list:
                increment_list.append(trade)
        return increment_list

    def _fetch_trade_data(self, order_id):
        """
        Fetch trade data.

        Args:
            order_id(string): order Id.
        """
        return fetch_trade_from_pms(self.account_id, order_id)

    def _fetch_order_data(self, order_id):
        """
        Fetch trade data.

        Args:
            order_id(string): order Id.
        """
        return fetch_order_from_pms(self.account_id, order_id)


if __name__ == '__main__':
    # 成交信息
    aid = '2593e2135e80435eba6661f5301c8e8c'
    test_id = "deb51fb6-2342-11e8-9688-0800271b3dfa"
    trade_cache = TradesSubscriber(aid)
    trade_cache.put_order(test_id)

    for item in trade_cache.fetch_trade():
        # time.sleep(1)
        print item
