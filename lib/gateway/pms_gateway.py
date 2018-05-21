# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: PMS gateway file
#   Author: Myron
# **********************************************************************************#
from utils.dict_utils import (
    DefaultDict,
    CompositeDict
)
from . base_gateway import BasePMSGateway
from .. configs import logger
from .. trade import (
    OrderState,
    OrderStateMessage
)


class PMSGateway(BasePMSGateway):
    """
    组合管理模块

    * 管理账户的持仓信息
    * 管理账户的订单委托信息
    * 管理账户的成交回报信息
    """
    def __init__(self, clock=None, accounts=None, data_portal=None,
                 position_info=None, initial_value_info=None,
                 order_info=None, trade_info=None, benchmark_info=None,
                 total_commission_info=None, event_engine=None,
                 ctp_gateway=None):
        """
        组合管理配置

        Args:
            clock(clock): 时钟
            accounts(dict): 账户管理
            data_portal(data_portal): 数据模块
            position_info(dict): 账户持仓信息 |-> dict(account: dict(date: dict))
            initial_value_info(dict): 初始权益信息 |-> dict(account: dict)
            order_info(dict): 订单委托 |-> dict(account: dict(order_id: order))
            trade_info(dict): 成交记录 |-> dict(account: dict(date: list))
            benchmark_info(dict): 用户对比权益曲线 |-> dict(account: string)
            total_commission_info(dict): 手续费记录　｜-> dict(account: dict(date: float))
            event_engine(obj): event engine
            ctp_gateway(obj): subscriber gateway.
        """
        super(PMSGateway, self).__init__()
        self.clock = clock
        self.accounts = accounts
        self.data_portal = data_portal
        self.position_info = position_info or DefaultDict(DefaultDict(dict))
        self.initial_value_info = initial_value_info or DefaultDict(dict)
        self.order_info = order_info or DefaultDict(dict)
        self.trade_info = trade_info or DefaultDict(DefaultDict(list))
        self.benchmark_info = benchmark_info or dict()
        self.total_commission_info = total_commission_info or DefaultDict(DefaultDict(0))
        self.event_engine = event_engine
        self.ctp_gateway = ctp_gateway
        self._account_name_id_map = {account: config.account_id for account, config in self.accounts.iteritems()}
        self._account_id_name_map = {config.account_id: account for account, config in self.accounts.iteritems()}

    @classmethod
    def from_config(cls, clock, sim_params, data_portal, accounts=None, event_engine=None, ctp_gateway=None):
        """
        从配置中生而成 PMS Gateway
        """
        position_info = DefaultDict(DefaultDict(dict))
        initial_value_info = DefaultDict(dict)
        total_commission_info = DefaultDict(DefaultDict(0))
        benchmark_info = dict()
        accounts = accounts or sim_params.accounts
        for account, config in accounts.iteritems():
            account_id = config.account_id
            benchmark_info[account_id] = sim_params.major_benchmark
        return cls(clock=clock, accounts=accounts, data_portal=data_portal,
                   position_info=position_info, initial_value_info=initial_value_info,
                   benchmark_info=benchmark_info, total_commission_info=total_commission_info,
                   event_engine=event_engine, ctp_gateway=ctp_gateway)

    def send_order(self, order, account_id=None):
        """
        Send order event.

        Args:
            order(obj): order object
            account_id(string): account id
        """
        logger.info('[PMS Gateway] [Send order] account_id: {}, order_id: {}, '
                    'order submitted.'.format(account_id, order.order_id))
        order.state = OrderState.ORDER_SUBMITTED
        order.state_message = OrderStateMessage.OPEN
        self.order_info[account_id][order.order_id] = order
        logger.info('[PMS Gateway] [Send order] account_id: {}, order_id: {}, '
                    'subscribe trade response of current order.'.format(account_id, order.order_id))
        self.ctp_gateway.trader_gateway.send_order(order)

    def cancel_order(self, order_id, account_id=None):
        """
        Cancel order event.

        Args:
            order_id(string): order id
            account_id(string): account id
        """
        target_order = self.order_info[account_id].get(order_id)
        if target_order is None:
            logger.warn('[PMS Gateway] [Cancel order] account_id: {}, order_id: {}, '
                        'can not find order.'.format(account_id, order_id))
            return
        target_order.state = OrderState.CANCELED
        target_order.state_message = OrderStateMessage.CANCELED
        logger.info('[PMS Gateway] [Cancel order] account_id: {}, order_id: {}, '
                    'order cancelled.'.format(account_id, order_id))

    def deal_with_trade(self, trade=None, **kwargs):
        """
        Deal with trade event.
        """
        logger.info('[PMS Gateway] [deal with trade] trade_id: {}, publish on_trade.'.format(trade.exchange_trade_id))
        account_id, order_id = trade.account_id, trade.order_id
        self.trade_info[account_id][order_id].append(trade)

    def deal_with_order(self, order_data, **kwargs):
        """
        Deal with order.

        Args:
            order_data(dict): order data item
        """
        account_id = order_data['accountId']
        order_id = order_data['extOrdId']
        order = self.order_info[account_id].get(order_id)
        if order:
            order.update_from_subscribe(item=order_data)
        else:
            logger.warn('[PMS Gateway] [deal with order] account_id: {}, order_id: {}, '
                        'no relevant order in record.'.format(account_id, order_id))

    def get_positions(self, account_id):
        """
        Get positions.

        Args:
            account_id(string): account id
        """
        return self.ctp_gateway.query_position_detail(account_id)

    def get_orders(self, account_id):
        """
        Get orders.

        Args:
            account_id(string): account id
        """
        return self.order_info[account_id]

    def get_trades(self, account_id):
        """
        Get trades.

        Args:
            account_id(string): account id
        """
        return self.trade_info[account_id]

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
            positions = self.position_info[account].get(info_date, dict())
            trades = self.trade_info[account].get(info_date, list())
            total_commission = self.total_commission_info[account].get(info_date)
            previous_date = self.data_portal.calendar_service.get_direct_trading_day(info_date, 1, forward=False)
            previous_positions = \
                self.position_info[account].get(previous_date, self.initial_value_info[account]['positions'])
            zipped_data[account]['previous_positions'] = previous_positions
            zipped_data[account]['initial_value'] = self.initial_value_info[account]
            zipped_data[account]['orders'] = orders
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
