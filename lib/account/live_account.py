# -*- coding: utf-8 -*-

"""
live_account.py

virtual account class for live trading

@author: yudi.wu
"""
import traceback
import uuid
from copy import deepcopy, copy
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from .. account.account import AccountConfig, AccountManager
from .. account.stock_account import StockAccount
from .. api import api_method, set_account_instance
from .. context.strategy import TradingStrategy
from .. context.parameters import SimulationParameters
from .. context.context import Context
from .. const import PRESET_KEYARGS, TRADE_ESSENTIAL_FIELDS_DAILY
from .. data import TradingCalendar
from .. data.market_service import MarketService
from .. data.calendar_service import CalendarService
from .. data.signal import signal
from .. trade.blotter import Blotter
from .. trade.position import Position as QuartzPosition
from .. trade.portfolio import StockPortfolio
from .. trade.order import Order, OrderState
from .. trade.cost import Commission, Slippage
from .. backtest import get_backtest_service, MarketRoller
from .. utils.data_utils import get_invalid_universe
from .. utils.datetime_utils import (
    normalize_date,
    get_minute_bars,
    previous_trading_day,
    get_direct_trading_day_list,
)
from .. utils.error_utils import WarningCenter, BacktestInputError
from .. utils.log_utils import Logger
from .. utils.privilege_utils import is_pro_user
from .. const import DEFAULT_ACCOUNT_NAME


if is_pro_user():
    from .. universe import Factor_pro as Factor
else:
    from .. universe import Factor

## 如下模块会在handle_data中使用到，请勿删除
from .. universe import set_universe, StockScreener, IdxCN, IndSW, IndZJH, IndZZ, Universe as DynamicUniverse
from .. data import Signal, SignalGenerator
from .. utils.special_trading_days import Weekly, Monthly
##

TAG = 'File "<string>", '

def beautify_traceback_global(msg):
    index = msg.find(TAG)
    if index >= 0:
        return msg[index + len(TAG):].replace('<module>', 'code')
    return msg

DAILY_EARLIEST_START = normalize_date('2006-01-01')
INTRADAY_EARLIEST_START = normalize_date('2009-01-01')
MINUTE_BARS = get_minute_bars()
WARNER = WarningCenter()

CODE_OK = 10
CODE_PARAMETER_ERROR = 11
CODE_EMPTY_UNIVERSE = 12
CODE_INVALID_STRG = 13
CODE_DATA_ERROR = 14
CODE_INITIALIZE_ERROR = 15
CODE_HANDLEDATA_ERROR = 16
CODE_DEFINE_ERROR = 17

ERROR_MAP = {
    CODE_OK: "OK",
    CODE_PARAMETER_ERROR: "invalid parameter",
    CODE_EMPTY_UNIVERSE: "account.universe is empty",
    CODE_INVALID_STRG: "invalid strategy function",
    CODE_DATA_ERROR: "data load error",
    CODE_INITIALIZE_ERROR: "initialize() internal error",
    CODE_HANDLEDATA_ERROR: "handle_data() internal error",
    CODE_DEFINE_ERROR: "define global error",
}

PMS_ORDER_STATUS_MAP = {
    "FORCE_CLOSE": OrderState.CANCELED,
    "PROCESSING": OrderState.ORDER_SUBMITTED,
    "WORKING": OrderState.OPEN,
    "PARTIAL_COMPLETE": OrderState.PARTIAL_FILLED,
    "COMPLETE": OrderState.FILLED,
    "PENDING_CANCEL": OrderState.CANCEL_SUBMITTED,
    "PARTIAL_CANCELLED": OrderState.CANCELED,
    "CANCELLED": OrderState.CANCELED,
    "ERROR": OrderState.ERROR,
    "CLOSED": OrderState.PARTIAL_FILLED,
}

ORDER_NORMAL = 1
ORDER_IGNORE = 2
ORDER_INVALID = 3
ORDER_NOT_COMPLIANCE = 4
ORDER_TO_CANCEL = 5


class LiveAccount(Context):
    """
    虚拟账户，包含如下属性

    * self._sim_params：回测参数
    * self._strategy: 回测策略
    * self._commission：手续费标准
    * self._slippage：滑点标准
    * self._freq：策略粒度
    * self._universe：策略证券池
    * self._current_date：当前回测日期
    * self._previous_date：当前回测日期的前一交易日
    * self._current_minute：当前回测分钟线（仅对日内策略有效）
    * self._current_universe：当前回测日期的动态证券池（包含停牌）
    * self._reference_price：参考价，一般使用的是上一日收盘价或者上一分钟收盘价
    * self._reference_return：参考收益率，一般使用的是上一日收益率
    * self._reference_portfolio_value：参考投资策略价值，使用参考价计算
    * self._blotter：下单指令列表
    * self._record：输出记录
    * self._daily_history：日线历史数据
    * self._intraday_history：分钟线历史数据
    * self._intraday_index_dict：分钟线索引字典
    * self.daily_trigger_time：日线策略模拟交易调仓时点
    """

    def __init__(self, sim_params=None, strategy=None, **kwargs):
        sim_params = SimulationParameters() if not sim_params else sim_params
        super(LiveAccount, self).__init__(sim_params, strategy, **kwargs)
        self._sim_params = None
        self._trading_calendar = None
        self._data = None
        self._strategy = None
        self._refresh_rate = None
        self._freq = None
        self._current_date = None
        self._previous_date = None
        self._current_minute = None
        self._current_universe = None
        self._record = {}
        self._reference_price = None
        self._reference_return = None
        self._reference_portfolio_value = None
        self._more_banlist = []
        self._self_defined_universe = []
        self.days_counter = 0
        self.daily_trigger_time = None
        self.trigger_bars = []
        self.trigger_days = []
        self.market_service = None
        self.universe_service = None
        self.calendar_service = None
        self.asset_service = None
        self.backtest_data_roller = None


    def from_code(self, code_input='', tradingtype=None, latest_trading_date=None, logger=None):
        # set environment variables
        set_account_instance(self)
        self.tradingtype = tradingtype
        from ..api import order, order_to, order_pct, order_pct_to, observe, cancel_order, get_order, get_orders
        try:
            from .. api import (
                winsorize,
                normalize_l1,
                standardize,
                neutralize,
                long_only,
                simple_long_only,
                normalize_code,
            )
            log = logger if logger is not None else Logger()
            log._set_account(self)
            self.logger = log
        except:
            pass

        for item in locals().keys():
            if item != 'self' and item != 'code_input':
                globals()[item] = locals()[item]

        try:
            exec code_input in globals(), globals()
        except:
            c = CODE_DEFINE_ERROR
            return (c, ERROR_MAP[c] + "\n\n" + beautify_traceback_global(traceback.format_exc()))

        # format inputs

        start = latest_trading_date
        benchmark = '000300.ZICN'

        if 'universe' not in globals():
            c = CODE_PARAMETER_ERROR
            return (c, ERROR_MAP[c] + "\n\n" + beautify_traceback_global(traceback.format_exc()))

        for key, value in PRESET_KEYARGS.items():
            if key not in globals():
                globals()[key] = value
        for key, value in {'commission': Commission(), 'slippage': Slippage()}.iteritems():
            if key not in globals():
                globals()[key] = value

        try:
            if freq not in 'dm':
                raise BacktestInputError('\"freq\" must be \"d\" or \"m\"!')
            self._freq = freq
        except:
            self._freq = 'd'

        try:
            global accounts
            self._sim_params = SimulationParameters(
                start=start,
                end=start,
                benchmark=benchmark,
                universe=universe,
                capital_base=capital_base,
                security_base=security_base,
                security_cost=security_cost,
                refresh_rate=refresh_rate,
                freq=self._freq,
                max_history_window=max_history_window,
                isbacktest=0,
            )
            account_manager = AccountManager(accounts)
            self.account_manager = account_manager if account_manager is not None else AccountManager()
            sim_params = self._sim_params
            self.account_manager.register_accounts(self, sim_params)
            if not self.account_manager.registered_accounts:
                accounts = {'default': AccountConfig(
                    'security', capital_base=sim_params.capital_base, commission=sim_params.commission,
                    slippage=sim_params.slippage, margin_rate=sim_params.margin_rate,
                    amount_base=sim_params.security_base, cost_base=sim_params.security_cost)}
                self.account_manager.register_accounts(self, sim_params, accounts=accounts)
            self.compatible_account = self.account_manager.compatible_account
            self.registered_accounts = self.account_manager.registered_accounts
            self.registered_accounts_params = self.account_manager.registered_accounts_params

            self.calendar_service, self.asset_service, self.universe_service, self.market_service = \
                get_backtest_service(self._sim_params)
            self.compatible_account._calendar_service = self.calendar_service
            self.compatible_account._asset_service = self.asset_service
            self.compatible_account._universe_service = self.universe_service
        except:
            c = CODE_PARAMETER_ERROR
            return (c, ERROR_MAP[c] + "\n\n" + beautify_traceback_global(traceback.format_exc()))

        try:
            self._strategy = TradingStrategy(initialize, handle_data)
        except:
            c = CODE_INVALID_STRG
            return (c, ERROR_MAP[c])

        try:
            self._refresh_rate = refresh_rate
        except:
            self._refresh_rate = (1, 1)

        self._user_variable = []
        origin = set(dir(self))
        try:
            self._strategy.initialize(self)
            current = set(dir(self))
            self._user_variable = list(current - origin)
            signal.name2id = {}
        except:
            c = CODE_INITIALIZE_ERROR
            return (c, ERROR_MAP[c] + '\n\n' + beautify_traceback_global(traceback.format_exc()))


        self._blotter = Blotter()
        self._position = None
        self._daily_history = None
        self._intraday_history = None
        self._intraday_index_dict = None
        self._ban_level = 'l0'

        try:
            if not isinstance(commission, Commission):
                raise BacktestInputError('Key word commission must be an instance of Commission!')
            self._commission = commission
        except:
            self._commission = Commission()

        try:
            if not isinstance(slippage, Slippage):
                raise BacktestInputError('Key word slippage must be an instance of Slippage!')
            self._slippage = slippage
        except:
            self._slippage = Slippage()

        if latest_trading_date is None:
            latest_trading_date = datetime.today()

        if isinstance(self._refresh_rate, int):
            end = start + timedelta(days=self._refresh_rate + 32)
        else:
            end = start + timedelta(days=32)
        try:
            self._trading_calendar = TradingCalendar(start=start, end=end)
            self._trading_calendar._parse_refresh_rate(self._refresh_rate, self._freq)
        except:
            c = CODE_INITIALIZE_ERROR
            return (c, ERROR_MAP[c] + '\n\n' + beautify_traceback_global(traceback.format_exc()))

        self.trigger_days = self._trading_calendar._trigger_days
        self.trigger_bars = self._trading_calendar._trigger_mins

        c = CODE_OK
        return (c, ERROR_MAP[c])

    # ===================== control methods =======================

    def is_adjustment_date(self, date):
        return date in self.trigger_days

    def variables_to_save(self):
        """
        模拟交易需要保存的Context内部变量列表
        """
        return ['compatible_account', 'universe_service'] + self.get_user_variable()

    def is_variable_user_defined(self, key):
        if key.startswith('_') or key in ['daily_trigger_time', 'signal_generator']:
            return False
        return True

    def get_user_variable(self):
        result = []
        for var in self._user_variable:
            if var not in ['daily_trigger_time', 'signal_generator', 'compatible_account', 'universe_service']:
                result.append(var)
        return result

    def refresh_daily_attribute(self, date, days_count=None):
        """
        在每个交易日开始之前刷新日期和blotter，每个交易日都执行一次，会在所有需要调用DataAPI的方法之前调用

        :param datetime date: 日期，一般为回测的当前交易日
        """

        date = normalize_date(date)

        self._current_date = date

        if days_count is None:
            self.days_counter += 1
        else:
            self.days_counter = days_count

        self._blotter.reset()

        c = CODE_OK
        return (c, ERROR_MAP[c])

    def refresh_daily_data(self):
        try:
            tick_roller_signature = None
            previous_date = previous_trading_day(self._current_date)
            self.calendar_service = CalendarService(self.sim_params.start,
                                                    max(self._current_date, self.sim_params.start),
                                                    self.sim_params.max_history_window_daily)
            self.universe_service.reload_universe([previous_date, self._current_date])
            self.asset_service.update_with_symbols(self.universe_service.full_universe)
            self.market_service = MarketService.create_with_service(self.asset_service, self.universe_service,
                                                                    calendar_service=self.calendar_service)
            signal_flag = False
            if hasattr(self, 'signal_generator'):
                signal_flag = True
                self.market_service.stock_market_data.set_factors(self.signal_generator.field)

            self.market_service.rolling_load_daily_data(self.calendar_service.all_trading_days[:-1])

            if signal_flag:
                self.compute_signals(trading_days=self.calendar_service.all_trading_days)
                self.signal_result = self.signal_generator.get_signal_result(self._current_date, self.current_universe)

            if self._freq == 'm':
                t = (self._sim_params.max_history_window_minute - 1) // 241 + 1
                intraday_history_days = get_direct_trading_day_list(previous_date, t, False)
                self.market_service.rolling_load_minute_data(intraday_history_days,
                                                             max_cache_days=len(intraday_history_days) + 1)
            # 更新属性
            self._previous_date = previous_date
            self._daily_history = self.market_service.slice(symbols='all', fields=TRADE_ESSENTIAL_FIELDS_DAILY,
                                                            end_date=self._previous_date, style='ast')

            # 合规部分
            self.universe_service.set_banlist(level='l1', ban_list=get_invalid_universe(self, level=1))
            self._ban_level = 'l1'
            if self.tradingtype in ('fund', 'contest'):
                more_banlist = get_invalid_universe(self, level=4)
                self.universe_service.set_banlist(level='l4', ban_list=more_banlist)
            else:
                more_banlist = []

            self.more_banlist = more_banlist

            if self._freq == 'm':
                market_data = self.market_service.stock_market_data or self.market_service.fund_market_data \
                              or self.market_service.index_market_data
                index = ['{} {}'.format(dt, minute) for dt in
                         market_data.minute_bars['closePrice'].index.values
                         for minute in MINUTE_BARS]
                self._intraday_index_dict = {time: i for i, time in enumerate(index)}
                tick_roller_signature = True
                current_date = self._current_date.strftime('%Y-%m-%d')
                columns = ['closePrice', 'turnoverVol', 'lowPrice', 'highPrice', 'openPrice']
                index = self._daily_history['closePrice'].columns
                data = {column: map(lambda x: [x], self._daily_history[column].iloc[-1,:].tolist())
                        for column in columns}
                current_dataframe = pd.DataFrame(data, index=index, columns=columns)
                current_dataframe['tradeTime'] = [['{} {}'.format(current_date, '09:30')]]*current_dataframe.index.size
                current_dataframe['barTime'] = [['09:30']]*current_dataframe.index.size
                self._intraday_history = {current_date: current_dataframe}

            backtest_data_roller = MarketRoller(
                universe=list(set(self.universe_service.full_universe)),
                market_service=self.market_service,
                trading_days=[self.previous_date, self.current_date],
                daily_bar_loading_rate=2,
                minute_bar_loading_rate=2,
            )

            daily_data = backtest_data_roller.prepare_daily_data(self._current_date)
            self.update_info(date=self._current_date, minute='09:30', previous_date=self._previous_date,
                             daily_data=daily_data, minute_transact_data=self._intraday_history)

        except Exception as e:
            c = CODE_DATA_ERROR
            print traceback.format_exc()
            return (c, ERROR_MAP[c] + '\n\n' + str(e))

        c = CODE_OK
        return (c, ERROR_MAP[c])

    def refresh_intraday_attribute(self, minute):
        """
        在每条分钟线开始之前刷新分钟和blotter，每次触发分钟线都执行一次，会在所有需要调用DataAPI的方法之前调用

        :param str minute: 分钟，一般为日内回测的当前分钟
        """
        self._current_minute = minute
        self._blotter.reset_current_blotter()
        self.update_info(minute=minute)
        # self._blotter = {}
        # self._order_state_table = {state: set() for state in OrderStatus.ALL}

        c = CODE_OK
        return (c, ERROR_MAP[c])

    def refresh_intraday_data(self, barTime, data):
        """
        在每条分钟线开始之前刷新self.data_ind到最近的情况，每次分钟线数据推送都执行一次，会在所有需要调用DataAPI的方法之前调用

        从liyuan处获取的data结构为：
            {'closePrice': {'000001.XSHE': 1.0, '600000.XSHG': 2.0, ...},
             'openPrice': {'000001.XSHE': 1.0, '600000.XSHG': 2.0, ...},
             ...}

        :param str barTime: 分钟
        :param dict data: 数据
        """

        try:
            timestamp = "{} {}".format(self.current_date.strftime("%Y-%m-%d"), barTime)
            self.tick_roller.push_tick(barTime, timestamp, self.current_date, data)

            # 缺失数值补充，如果价格缺失，则使用前一根分钟线，如果当日没有分钟线，则是用前一日收盘价
            data = deepcopy(data)
            ast_data = self.tick_roller.slice([self.previous_date, self.current_date],
                                              end_time=timestamp, time_range=1, style='ast', symbols='all')
            for stk in self.universe_service.full_universe:
                if stk not in data['closePrice']:
                    for attr in data.keys():
                        if stk in ast_data[attr]:
                            data[attr][stk] = (barTime, ast_data[attr][stk][0])
                        else:
                            data[attr][stk] = (barTime, self._daily_history['closePrice'][stk]
                                               .loc[self.previous_date.strftime('%Y-%m-%d')])
            if not self.compatible_account._intraday_data:
                self.compatible_account._intraday_data = {self.current_minute: data['closePrice']}
            else:
                self.compatible_account._intraday_data[self.current_minute] = data['closePrice']
        except Exception as e:
            c = CODE_DATA_ERROR
            print traceback.format_exc()
            return (c, ERROR_MAP[c] + '\n\n' + e.message)

        c = CODE_OK
        return (c, ERROR_MAP[c])

    def update_position(self, new_cash, new_secpos, avail_secpos, holding_price=None):
        """根据PMS最新持仓状态更新account属性以备handle_data使用"""

        if sorted(holding_price) != sorted(new_secpos):
            c = CODE_DATA_ERROR
            return (c, ERROR_MAP[c] + "\n\n Security position is not match with holding_price!")

        secpos = {}
        seccost = {}
        for s in new_secpos:
            if s[0] in '56':
                s += '.XSHG'
            elif s[0] in '03':
                s += '.XSHE'
            elif s[0] == '1' and s[1] in '56789':
                s += '.XSHE'
            else:
                continue
            secpos[s] = new_secpos[s[:6]]
            seccost[s] = holding_price[s[:6]]
        self._position = StockPortfolio(float(new_cash), QuartzPosition.from_dict(secpos, seccost))
        self.compatible_account._portfolio = self._position
        self._position.avail_secpos = {}
        securities = self._position.positions.keys()
        if len(set(securities) - set(self.universe_service.full_universe)):
            self.asset_service.add_init_asset(securities, self._sim_params._start, self._sim_params._end)
            self.universe_service.add_init_universe(securities)
            self.market_service = MarketService.create_with_service(asset_service=self.asset_service,
                                                                    universe_service=self.universe_service)

        for s in avail_secpos:
            if s[0] in '56':
                s += '.XSHG'
            elif s[0] in '03':
                s += '.XSHE'
            elif s[0] == '1' and s[1] in '56789':
                s += '.XSHE'
            else:
                continue

            if s in self.security_position:
                self._position.avail_secpos[s] = avail_secpos[s[:6]]

        c = CODE_OK
        return (CODE_OK, ERROR_MAP[CODE_OK])

    def update_blotter(self, orderlist):
        """
        element of orderlist looks like:
        {'End': True,
         'TradeSide': 'BUY',
         'Algorithm': 'MARKET',
         'ExchangeCode': 'XSHE',
         'ExternalOrderId': 'f32dfbc6-d134-4723-ab5d-c41d1fda0e3e',
         'MyOrderId': 由liyuan提供,
         'StartTime': None,
         'ExecutionAvgPrice': 10.03,
         'ExecutionQuantity': 100.0,
         'EndTime': None,
         'TickerSymbol': '000001',
         'State': 'WORKING',
         'Quantity': 100}
        """
        for PMSorder in orderlist:
            # order = Order(symbol=PMSorder['TickerSymbol'] + '.' + PMSorder['ExchangeCode'],
            #               amount=PMSorder['Quantity'],
            #               time=PMSorder['CreateTime'],
            #               # otype=PMSorder['Algorithm'].lower(),
            #               otype='limit' if PMSorder.has_key('Price') else 'market',
            #               price=PMSorder.get('Price', 0.), given_id=PMSorder["ExternalOrderId"])

            order = self._blotter.get_by_id(PMSorder['MyOrderId'])
            state = PMS_ORDER_STATUS_MAP.get(PMSorder['State'])
            if state is None:
                continue
            if order.state != state:
                self._blotter.change_order_state(order.order_id, state)

            order._filled_time = PMSorder.get('ExecutionTime', '')
            order._filled_amount = PMSorder.get('ExecutionQuantity', 0)
            order._transact_price = PMSorder.get('ExecutionAvgPrice', 0)

            # self._blotter[order.order_id] = order
            # self._order_state_table[order._state].add(order.order_id)

        c = CODE_OK
        return (CODE_OK, ERROR_MAP[CODE_OK])

    def handle_data(self):
        """
        更新_reference_price, _reference_return和_reference_portfolio_value，执行strg.handle_data(self, data)
        """

        try:
            for account_name, account in self.registered_accounts.iteritems():
                if isinstance(account, StockAccount):
                    account._get_reference_information()
            self._strategy.handle_data(self)
            c = CODE_OK
            return (c, ERROR_MAP[c])
        except:
            c = CODE_HANDLEDATA_ERROR
            return (c, ERROR_MAP[c] + '\n\n' + beautify_traceback_global(traceback.format_exc()))

        c = CODE_OK
        return (c, ERROR_MAP[c])

    @api_method
    def order(self, symbol, amount, price=0., otype='market', algo_params=None):
        """
        在handle_data(account, data)中使用，向account.blotter属性添加Order实例；指令含义为买入（卖出）数量为amount的证券symbol

        Args:
            symbol (str): 需要交易的证券代码，必须包含后缀，其中上证证券为.XSHG，深证证券为.XSHE
            amount (float or int): 需要交易的证券代码为symbol的证券数量，为正则为买入，为负则为卖出；程序会自动对amount向下取整到最近的整百
            price (float): 交易指令限价（仅分钟线策略可用）
            otype (str): 交易指令类型，可以为'market'或'limit'，默认为'market'，为limit时仅日内策略可用
        """

        if not isinstance(amount, (int, long, float)):
            raise ValueError("Order amount must be integer or float number!")
        if np.isnan(amount):
            raise ValueError("Order amount is nan!")
        elif amount > 0:
            amount = int(amount) / 100 * 100
        elif amount < 0:
            amount = -(-int(amount) / 100 * 100)

        if self._sim_params.freq == 'm':

            timestr = "{} {}".format(self.current_date.strftime('%Y-%m-%d'), self.current_minute)
            if otype == 'limit':
                if isinstance(price, (int, long, float)) and price > 0:
                    new_order = Order(symbol, amount, timestr, otype, price, algo_params=algo_params)
                else:
                    self.logger.warn('WARNING: price {} is not supported for limit order, will use market order instead'
                                .format(price))
                    new_order = Order(symbol, amount, timestr, algo_params=algo_params)
            else:
                new_order = Order(symbol, amount, timestr, algo_params=algo_params)
        else:
            if otype == 'limit':
                message = 'WARNING: limit order is not supported in daily trade, will use market order instead.'
                # WARNER.warn(message)
                self.logger.warn(message)
            timestr = "{} 09:30".format(self.current_date.strftime('%Y-%m-%d'))
            new_order = Order(symbol, amount, timestr, algo_params=algo_params)

        if symbol not in self.universe:
            new_order._state = OrderState.REJECTED

        self._blotter.add(new_order)

        if self._sim_params.freq == 'm':
            self._blotter.add_current_blotter(new_order)

        return new_order.order_id

    def order_to(self, symbol, amount, price=0., otype='market', algo_params=None):
        """
        在handle_data(account, data)中使用，向account.blotter属性添加Order实例；指令含义为买入（卖出）一定量的证券使得证券symbol交易后的数量为amount

        Args:
            symbol (str): 需要交易的证券代码，必须包含后缀，其中上证证券为.XSHG，深证证券为.XSHE
            amount (float or int): 需要交易的证券代码为symbol的证券数量，为正则为买入，为负则为卖出；程序会自动对amount向下取整到最近的整百
            price (float): 交易指令限价（仅分钟线策略可用）
            otype (str): 交易指令类型，可以为'market'或'limit'，默认为'market'，为limit时仅日内策略可用
        """

        if not isinstance(amount, (int, long, float)):
            raise ValueError('Exception in "LiveAccount.order_to": order amount must be integer or float number!')
        if np.isnan(amount):
            raise ValueError('Exception in "LiveAccount.order_to": order amount is nan!')
        elif amount < 0:
            raise ValueError('Exception in "LiveAccount.order_to": order amount must be positive!')

        amount = amount - self.security_position.get(symbol, 0)
        if amount > 0:
            amount = int(amount) / 100 * 100
        elif amount < 0:
            amount = -(-int(amount) / 100 * 100)
        return self.order(symbol, amount, price, otype, algo_params=algo_params)

    def order_pct(self, symbol, pct, price=0., otype='market', algo_params=None):
        """
        在handle_data(account, data)中使用，向account.blotter属性添加Order实例；指令含义为买入（卖出）价值为虚拟账户当前总价值的pct百分比的的证券symbol，仅限市价单

        Args:
            symbol (str): 需要交易的证券代码，必须包含后缀，其中上证证券为.XSHG，深证证券为.XSHE
            pct (float or int): 需要交易的证券代码为symbol的证券占虚拟账户当前总价值的百分比，范围为 -1 ~ 1，为正则为买入，为负则为卖出；程序会自动对amount向下取整到最近的整百
            price (float): 交易指令限价（仅分钟线策略可用）
            otype (str): 交易指令类型，可以为'market'或'limit'，默认为'market'，为limit时仅日内策略可用
        """

        if not -1 <= pct <= 1:
            raise ValueError('Exception in "LiveAccount.order_pct": percent must between -1 and 1!')
        if otype != 'market':
            raise ValueError('Exception in "LiveAccount.order_pct": order_pct only available for market order!')

        p = self.reference_price.get(symbol, 0)
        if np.isnan(p) or p == 0:
            self.logger.warn('No valid reference price of {} at {}! It might be a newly-issued security. '
                             'This order is ignored.'.format(symbol, self._current_date.strftime("%Y-%m-%d")))
            return

        amount = self.reference_portfolio_value * pct / self.reference_price[symbol]
        if np.isnan(amount):
            raise ValueError("Order amount is nan!")
        elif amount > 0:
            amount = int(amount) / 100 * 100
        elif amount < 0:
            amount = -(-int(amount) / 100 * 100)
        return self.order(symbol, amount, price, otype, algo_params=algo_params)

    def order_pct_to(self, symbol, pct, price=0., otype='market', algo_params=None):
        """
        在handle_data(account, data)中使用，向account.blotter属性添加Order实例；指令含义为买入（卖出）证券symbol使得其价值为虚拟账户当前总价值的pct百分比，仅限市价单

        Args:
            symbol (str): 需要交易的证券代码，必须包含后缀，其中上证证券为.XSHG，深证证券为.XSHE
            pct (float or int): 需要交易的证券代码为symbol的证券占虚拟账户当前总价值的百分比，范围为 -1 ~ 1，为正则为买入，为负则为卖出；程序会自动对amount向下取整到最近的整百
            price (float): 交易指令限价（仅分钟线策略可用）
            otype (str): 交易指令类型，可以为'market'或'limit'，默认为'market'，为limit时仅日内策略可用
        """

        if not 0 <= pct <= 1:
            raise ValueError('Percent must between 0 and 1!')
        if otype != 'market':
            raise ValueError('order_pct() only available on market order!')

        p = self.reference_price.get(symbol, 0)
        if np.isnan(p) or p == 0:
            self.logger.warn('No valid reference price of {} at {}! It might be a newly-issued security. '
                             'This order is ignored.'.format(symbol, self._current_date.strftime("%Y-%m-%d")))
            return

        amount = self.reference_portfolio_value * pct / self.reference_price[symbol] \
                 - self.security_position.get(symbol, 0)
        if np.isnan(amount):
            raise ValueError('Exception in "LiveAccount.order_pct_to": Order amount is nan!')
        elif amount > 0:
            amount = int(amount) / 100 * 100
        elif amount < 0:
            amount = -(-int(amount) / 100 * 100)
        return self.order(symbol, amount, price, otype, algo_params=algo_params)

    @api_method
    def cancel_order(self, order_id):
        """
        在handle_data(account)中使用，从Account实例中的account.blotter属性中撤回对应order_id的指令，表现为该order的state属性变为"OrderStatus.Cancelled"，并不再进行成交。

        Args:
            order_id: 需要撤回的订单id

        Returns:
            boolean: 订单是否取消成功
        """

        if self._sim_params.freq == 'd':
            message = "WARNING: cancel_order is not functional when freq=='d', it will be ommited."
            # WARNER.warn(message)
            self.logger.warn(message)
            return False

        if not self._blotter.has_order(order_id):
            # print "{} {} [WARN] There's no Order {}!".format(self._current_date.strftime("%Y-%m-%d"), self._current_minute, order_id)
            self.logger.warn("{} {} [WARN] There's no Order {}!".format(self._current_date.strftime("%Y-%m-%d"), self._current_minute, order_id))
            return False

        if self._blotter.in_status(order_id, OrderState.ACTIVE):
            order_to_cancel = self._blotter.get_by_id(order_id)
            if order_to_cancel.algo_params is not None:
                self.logger.warn(
                    "{} {} [INFO] Order {} cannot be cancelled. Order with algo options can not be cancelled.".format(
                        self._current_date.strftime("%Y-%m-%d"), self._current_minute, order_id))
            self._blotter.change_order_state(order_id, OrderState.CANCEL_SUBMITTED)
            self._blotter.add_current_blotter(self._blotter.get_by_id(order_id).external_id)
            print "{} {} [INFO] Order {} has been cancelled successfully.".format(self._current_date.strftime("%Y-%m-%d"), self._current_minute, order_id)
            return True
        else:
            # print "{} {} [INFO] Order {} cannot be cancelled.".format(self._current_date.strftime("%Y-%m-%d"), self._current_minute, order_id)
            self.logger.warn("{} {} [INFO] Order {} cannot be cancelled.".format(self._current_date.strftime("%Y-%m-%d"), self._current_minute, order_id))
            return False

    @property
    def _blotter(self):
        return self.compatible_account._blotter

    @_blotter.setter
    def _blotter(self, blotter):
        self.compatible_account._blotter = blotter