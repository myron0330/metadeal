# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: PMS lite manager file
#   Author: Myron
# **********************************************************************************#
from copy import copy
from configs import logger
from .. core.enums import AccountType
from .. trade.position import choose_position
from .. utils.dict_utils import (
    DefaultDict,
    CompositeDict
)
from .. utils.error_utils import Errors
from .. gateway.base_gateway import BaseGateway
from .. trade import OrderState, OrderStateMessage


def _get_positions_by(account_id, account_type=None, position_base=None):
    """
    Get positions by account id.

    Args:
        account_id(string): account id
        account_type(string): account type
        position_base(dict): position base

    Returns:
        dict: symbol-position
    """
    if position_base is not None:
        positions = dict()
        position_obj = choose_position(account_type)
        for symbol, amount in position_base.iteritems():
            if account_type == AccountType.digital_currency:
                positions[symbol] = position_obj(symbol=symbol, free=amount)
    else:
        raise NotImplementedError
    return positions


class PMSLite(BaseGateway):
    """
    组合管理模块

    * 管理账户的持仓信息
    * 管理账户的订单委托信息
    * 管理账户的成交回报信息
    """
    def __init__(self, clock=None, accounts=None, data_portal=None,
                 position_info=None, initial_value_info=None,
                 order_info=None, trade_info=None, benchmark_info=None,
                 total_commission_info=None):
        """
        组合管理配置

        Args:
            clock(clock): 时钟
            accounts(dict): 账户管理
            data_portal(data_portal): 数据模块
            position_info(dict): 账户持仓信息 |-> dict(account: dict(date: dict))
            initial_value_info(dict): 初始权益信息 |-> dict(account: dict)
            order_info(dict): 订单委托 |-> dict(account: dict(date: list))
            trade_info(dict): 成交记录 |-> dict(account: dict(date: list))
            benchmark_info(dict): 用户对比权益曲线 |-> dict(account: string)
            total_commission_info(dict): 手续费记录　｜-> dict(account: dict(date: float))
        """
        super(PMSLite, self).__init__(gateway_name='PMS_Lite')
        self.clock = clock
        self.accounts = accounts
        self.data_portal = data_portal
        self.position_info = position_info or DefaultDict(DefaultDict(dict))
        self.initial_value_info = initial_value_info or DefaultDict(dict)
        self.order_info = order_info or DefaultDict(DefaultDict(dict))
        self.pending_order_info = order_info or DefaultDict(DefaultDict(dict))
        self.trade_info = trade_info or DefaultDict(DefaultDict(list))
        self.benchmark_info = benchmark_info or dict()
        self.total_commission_info = total_commission_info or DefaultDict(DefaultDict(0))
        self._account_name_id_map = {account: config.account_id for account, config in self.accounts.iteritems()}
        self._account_id_name_map = {config.account_id: account for account, config in self.accounts.iteritems()}

    @classmethod
    def from_config(cls, clock, sim_params, data_portal, accounts=None):
        """
        从配置中生而成 PMSManager
        """
        position_info = DefaultDict(DefaultDict(dict))
        initial_value_info = DefaultDict(dict)
        initial_orders_info = DefaultDict(dict)
        total_commission_info = DefaultDict(DefaultDict(0))
        benchmark_info = dict()
        for account, config in accounts.iteritems():
            account_type = config.account_type
            position_base = config.position_base
            if account_type == AccountType.digital_currency:
                account_id = config.account_id
                position_info[account_id][sim_params.trading_days[0]] = _get_positions_by(account_id=account_id,
                                                                                          account_type=account_type,
                                                                                          position_base=position_base)
            else:
                raise Errors.INVALID_ACCOUNT_TYPE
            initial_value_info[account] = {
                'positions': copy(position_info[account_id][sim_params.trading_days[0]])
                                           }
            initial_orders_info[account_id] = dict()
            benchmark_info[account_id] = sim_params.major_benchmark
        return cls(clock=clock, accounts=accounts, data_portal=data_portal,
                   position_info=position_info, initial_value_info=initial_value_info,
                   benchmark_info=benchmark_info, total_commission_info=total_commission_info)

    def send_order(self, order, account_id=None):
        """
        Send order event.

        Args:
            order(obj): order object
            account_id(string): account id
        """
        logger.info('[PMS Lite] [Send order] account_id: {}, order_id: {}, '
                    'order submitted.'.format(account_id, order.order_id))
        order._state = OrderState.ORDER_SUBMITTED
        order._state_message = OrderStateMessage.OPEN
        self.order_info[account_id][self.clock.current_date][order.order_id] = order

    def cancel_order(self, order_id, account_id=None):
        """
        Cancel order event.

        Args:
            order_id(string): order id
            account_id(string): account id
        """
        target_order = self.order_info[account_id][self.clock.current_date].get(order_id)
        if target_order is None:
            logger.warn('[PMS Lite] [Cancel order] account_id: {}, order_id: {}, '
                        'can not find order.'.format(account_id, order_id))
            return
        target_order._state = OrderState.CANCELED
        target_order._state_message = OrderStateMessage.CANCELED
        logger.info('[PMS Lite] [Cancel order] account_id: {}, order_id: {}, '
                    'order cancelled.'.format(account_id, order_id))

    def on_trade(self, trade=None, **kwargs):
        """
        On trade event.
        """
        logger.info('[PMS Lite] [on trade]')
        pass

    def on_order(self, *args, **kwargs):
        """
        On order event.
        """
        logger.info('[PMS Lite] [on order]')
        pass

    def on_tick(self, *args, **kwargs):
        """
        On tick event.
        """
        logger.info('[PMS Lite] [on tick]')
        pass

    def on_log(self, *args, **kwargs):
        """
        On log event.
        """
        logger.info('[PMS Lite] [on log]')
        pass

    def on_order_book(self, *args, **kwargs):
        """
        On order book event.
        """
        logger.info('[PMS Lite] [on order book]')
        pass

    def handle_data(self, *args, **kwargs):
        """
        Handle data event.
        """
        logger.info('[PMS Lite] [handle data]')
        pass

    def synchronize_broker(self, feedback_info):
        """
        当日同步撮合回报

        Args:
            feedback_info(dict): 信息回报
        """
        current_date = self.clock.current_date
        for account in self.accounts:
            self.position_info[account].update(feedback_info['positions'][account])
            self.order_info[account][current_date].update(feedback_info['orders'][account][current_date])
            self.pending_order_info[account][current_date].update(feedback_info['orders'][account][current_date])
            self.trade_info[account][current_date] += feedback_info['trades'][account][current_date]
            self.total_commission_info[account].update(feedback_info['total_commission'][account])

    def get_portfolio_info(self, account=None, info_date=None):
        """
        获取当前时刻用户权益

        Args:
            account(string): account name
            info_date(datetime.datetime): Optional, 交易日期
        """
        zipped_data = CompositeDict()
        accounts = [account] if account else self.accounts.keys()
        for account in accounts:
            orders = self.order_info[account].get(info_date, dict())
            pending_orders = self.pending_order_info[account].get(info_date, dict())
            positions = self.position_info[account].get(info_date, dict())
            trades = self.trade_info[account].get(info_date, list())
            total_commission = self.total_commission_info[account].get(info_date)
            previous_date = self.data_portal.calendar_service.get_direct_trading_day(info_date, 1, forward=False)
            previous_positions = \
                self.position_info[account].get(previous_date, self.initial_value_info[account]['positions'])
            zipped_data[account]['previous_positions'] = previous_positions
            zipped_data[account]['initial_value'] = self.initial_value_info[account]
            zipped_data[account]['orders'] = orders
            zipped_data[account]['pending_orders'] = pending_orders
            zipped_data[account]['positions'] = positions
            zipped_data[account]['trades'] = trades
            zipped_data[account]['total_commission'] = total_commission
        return zipped_data

    def to_dict(self):
        """
        Returns:
            dict: PMS 信息汇总
        """
        return {
            'accounts': self.accounts,
            'orders': self.order_info,
            'positions': self.position_info,
            'initial_value': self.initial_value_info,
            'benchmark': self.benchmark_info,
            'total_commission': self.total_commission_info
        }
