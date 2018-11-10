# -*- coding: utf-8 -*-
from . order import Order, OrderState


class Blotter(object):
    """
    用户订单详情
    """

    def __init__(self):
        self._blotter_dict = {}
        self._current_blotter = []
        self._order_state_table = {state: set() for state in OrderState.ALL}

    def reset(self):
        """
        重置blotter内部信息
        """
        self._blotter_dict = {}
        self._order_state_table = {state: set() for state in OrderState.ALL}
        self._current_blotter = []

    def pending_blotters(self):
        """
        获取待成交的订单
        """
        return [od for od in self._blotter_dict.values() if od.state in OrderState.OPEN]

    def current_blotter(self):
        return self._current_blotter

    def reset_current_blotter(self):
        self._current_blotter = []

    def limit_current_blotter(self, size):
        for _order in self._current_blotter[size:]:
            if isinstance(_order, Order):
                order_id = _order.order_id
                self._order_state_table[self._blotter_dict[order_id].state].remove(order_id)
                del self._blotter_dict[order_id]
        self._current_blotter = self._current_blotter[:size]

    def add_current_blotter(self, new_order):
        self._current_blotter.append(new_order)

    def to_list(self):
        """
        获取blotter列表
        """
        return [self._blotter_dict[order_id] for order_id in sorted(self._blotter_dict)]

    def change_order_state(self, order_id, target_state=None, target_message=None):
        """
        更新当前order状态

        Args:
            order_id (str): 需要更新的order的ID
            target_state (str): 目标状态
            target_message (str): 目标状态提示

        """
        order = self._blotter_dict.get(order_id)
        if order is None:
            return
        if target_state is not None:
            self._order_state_table[order.state].remove(order_id)
            order._state = target_state
            self._order_state_table[order.state].add(order_id)
        if target_message is not None:
            order._state_message = target_message

    def get_by_id(self, order_id):
        """
        通过ID获取Order订单
        """
        return self._blotter_dict.get(order_id, None)

    def get_by_status(self, order_status):
        """
        通过OrderStatus获取订单列表
        """
        state_orders = set().union(*[self._order_state_table[s] for s in order_status])
        return state_orders

    def add(self, new_order):
        """
        在blotter中新增Order
        """
        self._blotter_dict[new_order.order_id] = new_order
        self._order_state_table[new_order.state].add(new_order.order_id)

    def limit(self, size):
        for order_id in sorted(self._blotter_dict.keys())[size:]:
            if order_id in self._current_blotter:
                self._current_blotter.remove(order_id)
            if self._blotter_dict[order_id] in self._current_blotter:
                self._current_blotter.remove(self._blotter_dict[order_id])
            self._order_state_table[self._blotter_dict[order_id].state].remove(order_id)
            del self._blotter_dict[order_id]

    def in_status(self, order_to_check, order_status):
        """
        检查order是否处在特定OrderStatus中
        """
        if isinstance(order_to_check, str) or isinstance(order_to_check, unicode):
            order_to_check = self._blotter_dict[order_to_check]
        if isinstance(order_status, list):
            return order_to_check.state in order_status
        else:
            return order_to_check.state == order_status

    def length(self):
        return len(self._blotter_dict)

    def has_order(self, order_id):
        return order_id in self._blotter_dict
