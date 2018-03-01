# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: BaseAccount FILE
# **********************************************************************************#
from abc import ABCMeta, abstractmethod
from .. context.parameters import SimulationParameters
from .. trade.order import OrderState
from .. utils.error_utils import WARNER, Errors
from .. utils.dict_utils import CompositeDict


class BaseAccount(object):

    __metaclass__ = ABCMeta

    def __init__(self, clock=None, data_portal=None, is_backtest=True):
        self.clock = clock
        self.data_portal = data_portal
        self.portfolio_info = CompositeDict()
        self.submitted_orders = list()
        self.submitted_cancel_orders = list()
        self._is_backtest = is_backtest

    @classmethod
    def from_config(cls, clock, sim_params, data_portal, is_backtest=True):
        """
        Generate account from global configs

        Args:
            clock(Clock): global clock
            sim_params(SimulationParameters): simulation parameters
            data_portal(DataPortal): data portal service
            is_backtest(boolean): whether or not back test
        """
        if not isinstance(sim_params, SimulationParameters):
            raise Errors.INVALID_SIM_PARAMS
        return cls(clock, data_portal, is_backtest=is_backtest)

    @property
    def blotter(self):
        """当前委托明细"""
        return self.portfolio_info['orders'].values()

    @property
    def cash(self):
        """当前现金"""
        return self.portfolio_info['cash']

    @property
    def position(self):
        """当前持仓组合信息"""
        return self.portfolio_info['positions']

    @property
    def portfolio_value(self):
        """账户权益"""
        return self.portfolio_info['portfolio_value']

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

    def cancel_order(self, order_id):
        """
        在handle_data(account)中使用，从Account实例中的account.blotter属性中撤回对应order_id的指令，
        表现为该order的state属性变为"OrderStatus.Cancelled"，并不再进行成交。
        Args:
            order_id: 需要撤回的订单id
        Returns:
            string: order_id
        """
        if self.clock.freq == 'd':
            message = "WARNING: cancel_order is not functional when freq=='d', it will be omitted."
            WARNER.warn(message)
            return
        self.submitted_cancel_orders.append(order_id)
        return order_id

    def get_trades(self):
        """
        Get trades list in current date.
        """
        return self.portfolio_info['trades']

    @abstractmethod
    def get_position(self, *args, **kwargs):
        """
        Get a specific position object.
        """
        return

    @abstractmethod
    def get_positions(self, *args, **kwargs):
        """
        Get all positions dict.
        """
        return

    # @property
    # def broker(self):
    #     """经纪人"""
    #     return self._broker
    #
    # @property
    # def commission(self):
    #     """佣金率"""
    #     return self._commission

    # def broker_transfer(self, cash, direction=1):
    #     """
    #     账户与 broker 之间现金转帐
    #
    #     Args:
    #         cash(int or float): 现金数量
    #         direction(int): 1: 向 broker 转入资金
    #                        -1: 向 broker 转出资金
    #     """
    #     # todo. transfer cash.
    #     if direction == 1:
    #         self._broker._cash = self._broker.cash + cash
    #     elif direction == -1:
    #         assert cash <= self._broker.get('cash'), 'Transferring cash must be less the available cash in broker! '
    #         self._broker._cash = self._broker.cash - cash
    #
    # def change_cash(self, cash):
    #     # todo. transfer cash.
    #     self.broker.portfolio.cash = cash
