# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: BaseAccount FILE
# **********************************************************************************#
from abc import ABCMeta
from .. context.parameters import SimulationParameters
from .. trade.order import OrderState
from .. utils.error_utils import Errors
from .. utils.dict_utils import CompositeDict


class BaseAccount(object):

    __metaclass__ = ABCMeta

    def __init__(self, clock=None, data_portal=None, event_engine=None, pms_gateway=None, account_id=None):
        self.clock = clock
        self.data_portal = data_portal
        self.portfolio_info = CompositeDict()
        self.submitted_orders = list()
        self.submitted_cancel_orders = list()
        self.event_engine = event_engine
        self.pms_gateway = pms_gateway
        self.account_id = account_id

    @classmethod
    def from_config(cls, clock, sim_params, data_portal, event_engine=None, pms_gateway=None, account_id=None):
        """
        Generate account from global configs

        Args:
            clock(Clock): global clock
            sim_params(SimulationParameters): simulation parameters
            data_portal(DataPortal): data portal service
            event_engine(obj): event engine
            pms_gateway(obj): pms gateway
            account_id(string): account id
        """
        if not isinstance(sim_params, SimulationParameters):
            raise Errors.INVALID_SIM_PARAMS
        return cls(clock, data_portal, event_engine=event_engine, pms_gateway=pms_gateway, account_id=account_id)

    @property
    def pending_blotter(self):
        """当前未成交订单"""
        return sorted(self.get_orders(state=OrderState.ACTIVE))

    def get_order(self, order_id):
        """
        通过order_id获取最新订单对象，并可通过order.state获取最新订单状态

        Args:
            order_id(string): order_id

        Returns:
            Order or None: order_id满足查询要求的Order对象，如果id不存在，则返回None
        """
        order = self.portfolio_info['orders'].get(order_id)
        if order is None:
            submitted = filter(lambda x: x.order_id == order_id, self.submitted_orders)
            if len(submitted) == 1:
                return submitted[0]
            elif not submitted and self.clock.freq == 'm':
                print "{} {} [WARN] There's no Order {}!".format(self.clock.current_date.strftime("%Y-%m-%d"),
                                                                 self.clock.current_minute, order_id)
            elif not submitted and self.clock.freq == 'd':
                print "{} 09:30 [WARN] There's no Order {}!".format(self.clock.current_date.strftime("%Y-%m-%d"),
                                                                    order_id)
            else:
                raise Errors.DUPLICATE_ORDERS
            return None
        return order

    def get_orders(self, state=OrderState.ACTIVE, symbol='all', **kwargs):
        """
        通过订单状态查询当前满足要求的订单，当前支持的订单状态可详见文档。
        Args:
            state: OrderState，不同订单状态可详见文档
            symbol: str or list，订单所属证券或证券列表，可使用'all'表示所有订单

        Returns:
            list: 满足state的Order对象列表
        """
        status = state if state else kwargs.get('status', OrderState.ACTIVE)
        symbol = [symbol] if not isinstance(symbol, list) and symbol is not 'all' else symbol
        status = [status] if not isinstance(status, list) else status
        if not set(status).issubset(set(OrderState.ALL)):
            raise ValueError('Invalid OrderStatus %s, '
                             'please refer to document for more information' % '/'.join(status))
        return [order for order_id, order in self.portfolio_info['orders'].iteritems()
                if order.state in status and symbol == 'all' or order.symbol in symbol]

    def order(self, *args, **kwargs):
        """
        Send order.
        """
        raise NotImplementedError

    def cancel_order(self, order_id):
        """
        Cancel order.

        Args:
            order_id(string): order id
        """
        raise NotImplementedError

    def get_trades(self):
        """
        Get trades list in current date.
        """
        raise NotImplementedError

    def get_positions(self):
        """
        Get all positions dict.
        """
        raise NotImplementedError

    def get_position(self, currency, **kwargs):
        """
        Get position of a specific currency.

        Args:
            currency(string): currency name
        """
        raise NotImplementedError
