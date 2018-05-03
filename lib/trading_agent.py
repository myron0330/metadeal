# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Trading Engine file
# **********************************************************************************#
import requests
import traceback
from copy import copy
from datetime import datetime
from . trading_base import *
from . event.event_base import EventType
from . gateway.trading_gateway import PaperTradingGateway
from .. context.context import Context
from .. context.parameters import SimulationParameters
from .. data.asset_service import AssetType
from .. utils.datetime_utils import get_direct_trading_day


class PaperTradingAgent(PaperTradingGateway):
    """
    交易调度引擎
    """
    def __init__(self, clock, sim_params,
                 strategy, data_portal, context,
                 account_manager, pms_lite,
                 broker_client, report_client,
                 market_roller, trading_scheduler,
                 sub_portfolio_info=None,
                 pms_host=None, pms_headers=None,
                 log=None, debug=False, event_engine=None):
        super(PaperTradingAgent, self).__init__()
        assert isinstance(sim_params, SimulationParameters)
        self.clock = clock
        self.sim_params = sim_params
        self.strategy = strategy
        self.data_portal = data_portal
        self.context = context
        self.account_manager = account_manager
        self.pms_lite = pms_lite
        self.broker_client = broker_client
        self.report_client = report_client
        self.market_roller = market_roller
        self.trading_scheduler = trading_scheduler
        self.sub_portfolio_info = sub_portfolio_info
        self.pms_host = pms_host
        self.pms_headers = pms_headers
        self.log = log
        self.debug = debug
        self.event_engine = event_engine
        self.trading_days_length = None
        self.current_minute_bars = None
        self._active = False

    @classmethod
    def from_config(cls, clock, sim_params, data_portal, strategy, account_manager,
                    pms_lite, broker_client, report_client, market_roller,
                    trading_scheduler, context=None, sub_portfolio_info=None,
                    pms_host=None, pms_headers=None, log=None, debug=False,
                    event_engine=None):
        calendar_service, asset_service, universe_service, market_service = \
            data_portal.calendar_service, data_portal.asset_service, \
            data_portal.universe_service, data_portal.market_service
        if context is None:
            context = Context(clock, sim_params, strategy,
                              market_service=market_service,
                              universe_service=universe_service,
                              asset_service=asset_service,
                              calendar_service=calendar_service,
                              market_roller=market_roller,
                              account_manager=account_manager)
        if context.signal_flag():
            if not set(context.signal_generator.field) <= set(market_service.stock_market_data.factors):
                market_service.stock_market_data.clear_data()
            market_service.stock_market_data.set_factors(context.signal_generator.field)
        return cls(clock, sim_params, strategy, data_portal, context, account_manager,
                   pms_lite, broker_client, report_client, market_roller,
                   trading_scheduler, sub_portfolio_info=sub_portfolio_info,
                   pms_host=pms_host, pms_headers=pms_headers, log=log, debug=debug,
                   event_engine=event_engine)

    def start(self):
        """
        PaperTradingAgent start.
        """
        self._active = True

    def stop(self):
        """
        PaperTradingAgent stop.
        """
        self._active = False

    def is_active(self):
        """
        Active or not.
        """
        return self._active

    def on_bar(self, bar, **kwargs):
        """
        On bar.

        Args:
            bar(obj): bar data
        """
        self._refresh_bar(bar)
        current_bars = \
            self.data_portal.market_service.minute_bar_map.get(self.clock.current_date.strftime('%Y-%m-%d'), list())
        self._update_clock(current_bars=current_bars)
        if self.clock.current_minute in self.trading_scheduler.trigger_minutes(current_bars):
            self.handle_data()
            self.send_orders()

    def on_portfolio(self, portfolio_info, **kwargs):
        """
        On portfolio.

        Args:
            portfolio_info(dict): portfolio info
        """
        self.pms_lite.synchronize_broker(feedback_info=portfolio_info)

    def register_handlers(self, event_engine):
        """
        Register handlers.
        """
        for event in EventType.paper_trading_events():
            event_engine.register_handlers(event, getattr(self, event))

    def prepare_initialize(self, **kwargs):
        """
        Prepare initialize
        """
        self.trading_scheduler.prepare(**kwargs)
        self.trading_days_length = len(self.trading_scheduler.trading_days())
        if self.context.signal_flag() and not self.context.market_service.stock_market_data.daily_bars:
            trading_days = self.context.calendar_service.all_trading_days
            self.context.market_service.rolling_load_daily_data(trading_days)

    def compute_signal_info(self, trading_days=None):
        """
        计算 signal 信息

        Args:
            trading_days(list of datetime.datetime): 全部交易日期
        """
        trading_days = trading_days or self.data_portal.calendar_service.all_trading_days
        if self.context.signal_flag():
            self.context.compute_signals(trading_days=trading_days)

    def rolling_load_daily_data(self, trading_days=None):
        """
        Rolling load daily data

        Args:
            trading_days(list of datetime.datetime): trading days
        """
        trading_days = trading_days or self.data_portal.calendar_service.all_trading_days
        if not trading_days:
            return
        self.data_portal.market_service.rolling_load_daily_data(trading_days)
        if self.context.signal_flag() and trading_days:
            self.context.compute_signals(trading_days=trading_days)

    def rolling_load_minute_data(self, trading_days=None):
        """
        Rolling load minute data

        Args:
            trading_days(list of datetime.datetime): trading days
        """
        if not trading_days:
            return
        max_cache_days = 2 * len(trading_days) + self.trading_scheduler.history_loading_window_m
        curr_trading_day, hist_trading_day = trading_days[-1], trading_days[:-1]
        self.data_portal.market_service.rolling_load_minute_data(hist_trading_day, max_cache_days=max_cache_days)
        self.data_portal.market_service.load_current_trading_day_bars(curr_trading_day, debug=self.debug)

    def pre_trading_day(self, date):
        """
        Pre trading day tasks: parse signal info, update daily data and adapt data to brokers.
        """
        date = date or self.clock.current_date
        self._publish_signal_info(date)
        previous_trading_day = self.trading_scheduler.previous_date(date)
        self.market_roller.prepare_daily_data(previous_trading_day)

    def pre_trading_minute(self, date, minute_bars=None):
        """
        Pre trading minute tasks: update minute data
        """
        current_date_str = date.strftime('%Y-%m-%d')
        current_minute_bars = \
            minute_bars or self.data_portal.market_service.minute_bar_map.get(current_date_str)
        self.current_minute_bars = current_minute_bars
        minute_window = self.trading_scheduler.history_loading_window_m
        self.market_roller.prepare_minute_data(current_date=date,
                                               extend_loading_days=minute_window)

    def handle_data(self):
        """
        Publish reference information and handle users data.
        """
        self.pms_lite.evaluate_portfolio(settle=False)
        self._publish_portfolio_info()
        self.strategy.handle_data(self.context)

    def publish_orders(self):
        """
        Publish user orders and cancel_orders by accounts.
        """
        cash_orders, submitted_orders, submitted_cancel_orders = dict(), dict(), dict()
        for account_name, account in self.account_manager.registered_accounts.iteritems():
            if account.cash_inout:
                cash_orders[account_name] = copy(account.cash_inout)
                account.cash_inout.clear()
            submitted_orders[account_name] = copy(account.submitted_orders)
            submitted_cancel_orders[account_name] = copy(account.submitted_cancel_orders)
            del account.submitted_orders[:]
            del account.submitted_cancel_orders[:]
        return cash_orders, submitted_orders, submitted_cancel_orders

    def send_orders(self, response=None):
        """
        Send orders
        """
        if response is None:
            _, submitted_orders, _ = self.publish_orders()
            response = orders_to_response(submitted_orders, self.sub_portfolio_info,
                                          registered_accounts=self.account_manager.registered_accounts)
        parameters = {
            'orders': response
        }
        self.event_engine.publish(EventType.event_order_response, **parameters)
        # pms_order_url = '{}/orders'.format(self.pms_host)
        # for securities_type, order_list in response.iteritems():
        #     if order_list:
        #         request_data = {
        #             'orders': order_list
        #         }
        #         target_order_url = '{}?securities_type={}'.format(pms_order_url, securities_type)
        #         try:
        #             requests.post(target_order_url, json=request_data, headers=self.pms_headers)
        #             self.log.info('#' * 50)
        #             self.log.info('{}: 下单正常，涉及订单为：')
        #             for order in order_list:
        #                 self.log.info(order)
        #         except:
        #             self.log.error('#' * 50)
        #             self.log.error('{}: 下单异常，涉及订单为:'.format(self.clock.now))
        #             for order in order_list:
        #                 self.log.error(order)
        #             self.log.error('{}\n'.format(traceback.format_exc()))
        return response

    def _publish_portfolio_info(self):
        """
        Publish portfolio info
        """
        portfolio_info = self.pms_lite.get_portfolio_info(info_date=self.clock.current_date)
        for account_name, account in self.account_manager.registered_accounts.iteritems():
            account.portfolio_info.update(portfolio_info[account_name])

    def _publish_signal_info(self, date):
        """
        Publish signal info

        Args:
            date(datetime.datetime): 交易日期
        """
        if self.context.signal_flag():
            self.context.signal_result = self.context.signal_generator.get_signal_result(
                date, self.context.get_universe(AssetType.EQUITIES, exclude_halt=True, with_position=True))

    def _refresh_bar(self, bar):
        """
        Refresh bar.
        """
        bar_minutes = map(lambda x: x[0], bar)
        current_minutes = self.data_portal.market_service.minute_bar_map[self.clock.current_date.strftime('%Y-%m-%d')]
        incremental_minutes, incremental_bars = list(), list()
        for _ in bar:
            _minute = _[0]
            if _minute in set(bar_minutes) - set(current_minutes):
                incremental_minutes.append(_minute)
                incremental_bars.append(_)
        self.data_portal.market_service.back_fill_rt_bar_times(date=self.clock.current_date,
                                                               bar_time_list=incremental_minutes)
        self.market_roller.back_fill_rt_data(current_trading_day=self.clock.current_date,
                                             rt_data=incremental_bars)

    def _update_clock(self, current_bars=None):
        """
        Update clock.
        """
        current_bars = current_bars or list()
        if self.debug:
            if current_bars:
                self.clock.update_time(minute=current_bars[-1])
        else:
            current_minute = datetime.now().strftime('%H:%M')
            if self.clock.current_minute != current_minute:
                self.clock.update_time(minute=current_minute)
        # change date to clearing date.
        if self.clock.current_minute > '16:00' and self.clock.current_date <= datetime.today():
            next_trading_date = get_direct_trading_day(self.clock.current_date, step=1)
            self.clock.update_time(previous_date=self.clock.current_date, current_date=next_trading_date)
