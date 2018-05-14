# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
# encoding: UTF-8
import logging
import sys
import threading
from copy import deepcopy
from datetime import datetime
from time import sleep

from quartz_futures.ctp.dependency.vnctptd import *

from quartz_futures.trade.futures_account import OrderBlotterRepo
from quartz_futures.trade.futures_account import PositionsRepo
from quartz_futures.utils.error_utils import CTPError
from quartz_futures.utils.calendar_utils import is_good_time_to_make_money
from quartz_futures.cache import redis_connection

order_type_map = {
    'market': '1',
    'limit': '2'
}

# 将多头开仓,多头平仓等转换为买入开仓,卖出开仓
# ('long', 'open'): ('buy', 'open'),
# ('long', 'close'): ('sell', 'close'),
# ('long', 'close_today'): ('sell', 'close_today'),
# ('short', 'open'): ('sell', 'open'),
# ('short', 'close'): ('buy', 'close')
# ('short', 'close_today'): ('buy', 'close_today')

direction_offset_map = {
    ('long', 'open'): ('0', '0'),
    ('long', 'close'): ('1', '1'),
    ('long', 'close_today'): ('1', '2'),
    ('short', 'open'): ('1', '0'),
    ('short', 'close'): ('0', '1'),
    ('short', 'close_today'): ('0', '2')
}


class TradeConnector(TdApi):
    """
    CTP交易服务器连接组件
    """

    def __init__(self, account_config=None):
        """Constructor"""
        super(TradeConnector, self).__init__()

        self.account_config = account_config

        self._request_id = 0

        self._brokerID = ''
        self._userID = ''
        self._frontID = 1
        self._sessionID = 0

        self._order_manager = None
        self._position_manager = None

        self.connected = False
        self.logged_in = False
        self.is_running = False

    def next_request_id(self):
        self._request_id += 1

        return self._request_id

    def connect_and_login(self):
        """
        连接CTP交易前置服务器.
        并且进行登陆

        """
        if self.logged_in:
            logging.debug(u'交易前置服务器已连接并登陆,不再重复连接')
            return

        self.connected = False
        self.logged_in = False

        logging.debug(u'开始连接交易前置服务器')

        # 在C++环境中创建MdApi对象，传入参数是希望用来保存.con文件的地址，测试通过
        self.createFtdcTraderApi('.')

        # 设置数据流重传方式，测试通过
        self.subscribePrivateTopic(1)
        self.subscribePublicTopic(1)

        broker_name = self.account_config.get('broker_name')
        ctp_td_address = str(self.account_config.get('td_address'))
        logging.debug(u'连接交易前置服务器.[broker_name:%s,trade_server_address:%s]' % (broker_name, ctp_td_address))

        # 注册前置机地址
        self.registerFront(ctp_td_address)

        # 初始化api，连接前置机
        self.init()
        sleep(3)

        #     # self.login()
        #
        # def login(self):
        ctp_broker_id = self.account_config.get('broker_id')
        ctp_user_id = self.account_config.get('user_id')
        ctp_password = self.account_config.get('password')

        self._brokerID = ctp_broker_id
        self._userID = ctp_user_id

        # 登陆，测试通过
        login_request = dict()
        login_request['UserID'] = ctp_user_id  # 参数作为字典键值的方式传入
        login_request['Password'] = ctp_password  # 键名和C++中的结构体成员名对应
        login_request['BrokerID'] = ctp_broker_id

        logging.debug(login_request)

        logging.debug(u'向交易前置服务器,发送登陆请求.[broker_id:%s/user_id:%s]' % (ctp_user_id, ctp_broker_id))
        req_id = self.next_request_id()
        return_code = self.reqUserLogin(login_request, req_id)

        if return_code < 0:
            logging.error(u'登陆交易前置服务器异常.[return_code:%s]' % return_code)
            raise CTPError('Fail to login to CTP Server')

        sleep(1)

        # 如果已经连接行情,则查询账户资金
        if self.connected:
            self.query_account_info()

    def send_order(self, order):
        """
        发送订单委托

        :param order:
        """
        order_id = order.order_id

        assert order_id

        request = dict()
        request['InstrumentID'] = order.symbol

        order_type = order.order_type
        if order_type != 'market':
            limit_price = float(order.order_price)
        else:
            limit_price = 0

        request['LimitPrice'] = limit_price
        request['VolumeTotalOriginal'] = order.order_amount

        # 订单类型, 1-市价,2-限价
        request['OrderPriceType'] = order_type_map.get(order_type, '1')

        direction, offset_flag = direction_offset_map.get((order.direction, order.offset_flag), ('1', '1'))
        request['Direction'] = direction
        request['CombOffsetFlag'] = offset_flag

        request['OrderRef'] = str(order_id)
        request['InvestorID'] = str(self._userID)
        request['UserID'] = str(self._userID)
        request['BrokerID'] = str(self._brokerID)

        request['CombHedgeFlag'] = '1'  # 投机单
        request['ContingentCondition'] = '1'  # 立即发单
        request['ForceCloseReason'] = '0'  # 非强平
        request['IsAutoSuspend'] = 0  # 非自动挂起
        request['TimeCondition'] = '3'  # 今日有效
        request['VolumeCondition'] = '1'  # 任意成交量
        request['MinVolume'] = 1  # 最小成交量为1

        request_id = self.next_request_id()

        logging.debug(u'向CTP服务器,发送订单委托：[request_id:%s,order_id:%s]\n%s' % (request_id, order_id, request))
        self.reqOrderInsert(request, request_id)

        return order_id

    def cancel_order(self, ticker, exchange, order_id):
        """
        撤单

        """
        request = dict()
        request['InstrumentID'] = ticker
        request['ExchangeID'] = exchange
        request['OrderRef'] = order_id
        request['FrontID'] = self._frontID
        request['SessionID'] = self._sessionID

        # 撤单标志
        request['ActionFlag'] = '0'
        request['BrokerID'] = self._brokerID
        request['InvestorID'] = self._userID

        self.reqOrderAction(request, self.next_request_id())

    def query_account_info(self):
        """
        查询账户当前信息

        """
        request = dict()
        request['BrokerID'] = self._brokerID
        request['InvestorID'] = self._userID

        logging.debug(u'发送查询账户信息的请求.[request:%s]' % request)
        self.reqQryTradingAccount(request, self.next_request_id())

        sleep(1)

    def query_position(self):
        """
        查询账户持仓

        """
        request = dict()
        request['BrokerID'] = self._brokerID
        request['InvestorID'] = self._userID

        logging.debug(u'发送查询账户持仓摘要的请求.[request:%s]' % request)
        self.reqQryInvestorPosition(request, self.next_request_id())

    def query_position_detail(self):
        """
        查询账户持仓

        """
        request = dict()
        request['BrokerID'] = self._brokerID
        request['InvestorID'] = self._userID

        logging.debug(u'发送查询账户持仓明细的请求.[request:%s]' % request)
        self.reqQryInvestorPositionDetail(request, self.next_request_id())

    def query_order_by_symbol(self, ticker=None):
        """
        查询当日订单委托

        :param ticker:
        """
        criteria = dict()
        if ticker:
            criteria['InstrumentID'] = ticker

        self.reqQryOrder(criteria, self.next_request_id())

    def query_trade_by_symbol(self, ticker=None):
        """
        查询当日成交记录

        :param ticker:
        """
        criteria = dict()
        if ticker:
            criteria['InstrumentID'] = ticker

        self.reqQryTrade(criteria, self.next_request_id())

    def query_settlement_info(self):
        """
        查询账户清算信息

        """
        self.reqQrySettlementInfo({}, self.next_request_id())

    def confirm_settlement_info(self):
        """
        确认每日清算单

        """
        request = dict()
        request['BrokerID'] = self._brokerID
        request['InvestorID'] = self._userID
        self.reqSettlementInfoConfirm(request, self.next_request_id())

    def get_trading_calendar(self):
        """
        查询交易日历

        """
        self.getTradingDay()

    def query_market_data(self):
        """
        查询最新市场快照数据

        """
        self.reqQryDepthMarketData({}, self.next_request_id())

    def query_instrument(self, ticker=None):
        """
        查询合约基础信息

        """
        criteria = dict()
        if ticker:
            criteria['InstrumentID'] = ticker

        self.reqQryInstrument(criteria, self.next_request_id())

    def query_instrument_margin_rate(self, ticker):
        """
        查询合约保证金率

        """
        request = dict()
        request['InstrumentID'] = ticker
        request['BrokerID'] = self._brokerID
        request['InvestorID'] = self._userID
        request['HedgeFlag'] = '1'
        logging.debug(u'查询合约保证金率.\n%s' % request)

        self.reqQryInstrumentMarginRate(request, self.next_request_id())

    def query_instrument_commission_rate(self, ticker):
        """
        查询合约保证金率

        """
        request = dict()
        request['InstrumentID'] = ticker
        request['BrokerID'] = self._brokerID
        request['InvestorID'] = self._userID
        logging.debug(u'查询合约保证金率.\n%s' % request)

        self.reqQryInstrumentCommissionRate(request, self.next_request_id())

    def request_authenticate(self, auth_code, product_info):
        """
        发送客户端认证信息

        """
        logging.debug(u'发送客户认证信息')

        authenticate_info = dict()
        authenticate_info['BrokerID'] = self._brokerID
        authenticate_info['UserID'] = self._userID
        authenticate_info['AuthCode'] = auth_code
        authenticate_info['UserProductInfo'] = product_info

        self.reqAuthenticate(authenticate_info, self.next_request_id())

    def stop(self):
        self.connected = False
        self.logged_in = False
        self.is_running = False

    ###################################################################################################################
    # 响应回调方法
    ###################################################################################################################
    def onFrontConnected(self):
        """
        服务器连接

        收到交易服务器连接成功消息。
        """
        self.connected = True

        logging.debug(u'与CTP交易前置服务器连接成功，开始登陆交易服务器')

        # 重新连接并登陆
        # self.login()

    def onFrontDisconnected(self, n):
        """服务器断开"""
        self.connected = False
        self.logged_in = False

        logging.debug(u'与CTP交易前置服务器断开连接.[n:%s]' % n)

        now = datetime.now().strftime('%H%M%s')

        if not is_good_time_to_make_money(now):
            logging.debug(u'当前非交易时间,停止与交易服务器的连接.[now:%s]' % now)
            self.stop()

    def onHeartBeatWarning(self, n):
        """"""
        logging.debug(u'心跳异常,出现报警.[n:%s]' % n)

    def onRspAuthenticate(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspUserLogin(self, data, error, n, last):
        """登陆回报"""
        self._frontID = data['FrontID']
        self._sessionID = data['SessionID']

        error_message = error.get('ErrorMsg', '')
        max_order_ref = data.get('MaxOrderRef', 0)

        self._request_id = int(max_order_ref) if max_order_ref else 1

        logging.debug(u'用户登陆CTP交易服务器.当前最大请求ID为%s.[user_id:%s/broker_id:%s/front_id:%s/'
                      u'session_id:%s/message:%s]\n%s\n%s'
                      % (self._request_id, self._userID, self._brokerID, self._frontID, self._sessionID,
                         error_message.decode('GBK'), data, error))

        self.logged_in = True

        logging.debug(u'收到CTP交易服务器反馈消息.[n:%s,last:%s]' % (n, last))
        logging.debug(data)
        logging.debug(error)

    def onRspUserLogout(self, data, error, n, last):
        """登出回报"""
        self.logged_in = False

        logging.debug(u'用户登出CTP交易服务器.[n:%s,last:%s]' % (n, last))
        logging.debug(data)
        logging.debug(error)

    def onRtnOrder(self, data):
        """报单回报"""
        status_message = data.get('StatusMsg', '')
        logging.debug(u'收到订单委托回报.StatusMsg:%s.\n%s' % (status_message.decode('GBK'), data))

        order_id = data.get('OrderRef', '')
        _order = OrderBlotterRepo.get_order_raw(order_id)

        if not _order:
            return

        status_event = data.get('OrderStatus', '')
        current_status = _order.get('status', '')
        next_status = next_order_status(current_status, status_event)

        logging.debug(u'更新订单状态.[order_id:%s,status_event:%s,current_status:%s,next_status:%s]'
                      % (order_id, status_event, current_status, next_status))

        _order['status'] = next_status

        OrderBlotterRepo.update_order(order_id, _order, scope='strategy')
        OrderBlotterRepo.update_order(order_id, _order)

    def onRtnTrade(self, data):
        """成交回报"""
        order_id = data.get('OrderRef', '')
        logging.debug(u'收到订单成交回报.order_id:%s.\n%s' % (order_id, data))

        sec_id = data.get('InstrumentID', '')
        transact_price = data.get('Price', '')
        transact_amount = data.get('Volume', '')
        transact_date = data.get('TradeDate', '')
        transact_time = data.get('TradeTime', '')

        _order = OrderBlotterRepo.get_order_raw(order_id)

        logging.debug(u'当前订单状态[order_id:%s,order:%s]' % (order_id, _order))

        if not _order:
            return

        # 更新原有订单数据
        direction = _order.get('direction', '')
        offset_flag = _order.get('offset_flag', '')
        current_status = _order.get('status', '')
        next_status = 'filled'

        logging.debug(u'更新订单状态.[order_id:%s,current_status:%s,next_status:%s]'
                      % (order_id, current_status, next_status))

        _order['status'] = next_status
        _order['transact_price'] = transact_price
        _order['transact_amount'] = transact_amount
        _order['filled_time'] = u'%sT%s' % (transact_date, transact_time)
        _order['filled'] = 1
        OrderBlotterRepo.update_order(order_id, _order)

        # 新增成交记录数据
        _trade = deepcopy(_order)
        OrderBlotterRepo.new_trade(order_id, _trade)

        # 更新原有持仓数据
        _position = PositionsRepo.get_position(sec_id, scope='strategy')

        logging.debug(u'更新策略持仓数据.[order_id:%s,sec_id:%s,current_position:%s]'
                      % (order_id, sec_id, _position))

        position_key_map = {
            'long': 'long_position',
            'short': 'short_position'
        }
        position_key = position_key_map[direction]

        if _position:
            _position = eval(_position)
            # 如果是开仓,则仓位增加
            if offset_flag == 'open':
                _position[position_key] = _position[position_key] + transact_amount

            # 如果是平仓,则仓位减少
            else:
                _position[position_key] = _position[position_key] - transact_amount
        else:
            _position = dict()
            _position[position_key] = transact_amount

        PositionsRepo.update_position(sec_id, _position, scope='strategy')

    def onRspQryOrder(self, data, error, n, last):
        """"""
        logging.debug(u'%s/%s/%s' % (n, last, data))
        logging.debug(error)

        order = dict()
        order['ticker'] = data.get('InstrumentID')
        order['open_interest'] = data.get('Position')
        order['holding_yesterday'] = data.get('YdPosition')
        order['holding_today'] = data.get('TodayPosition')
        order['direction'] = data.get('PosiDirection')
        order['open_today'] = data.get('OpenAmount')
        order['close_today'] = data.get('CloseVolume')

        redis_connection.update_order_blotter(order)

    def onRspQryTrade(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

        trade = dict()
        trade['ticker'] = data.get('InstrumentID')
        trade['open_interest'] = data.get('Position')
        trade['holding_yesterday'] = data.get('YdPosition')
        trade['holding_today'] = data.get('TodayPosition')
        trade['direction'] = data.get('PosiDirection')
        trade['open_today'] = data.get('OpenAmount')
        trade['close_today'] = data.get('CloseVolume')

        redis_connection.update_trade_blotter(trade)

    def onRspQryInvestorPosition(self, data, error, n, last):
        """持仓查询回报"""
        logging.debug(data)
        logging.debug(error)

        ticker = data.get('InstrumentID')
        position = dict()
        position['ticker'] = ticker
        position['open_interest'] = data.get('Position')
        position['holding_yesterday'] = data.get('YdPosition')
        position['holding_today'] = data.get('TodayPosition')
        position['direction'] = data.get('PosiDirection')
        position['open_today'] = data.get('OpenAmount')
        position['close_today'] = data.get('CloseVolume')

        PositionsRepo.update_position(ticker, position)

    def onRspQryTradingAccount(self, data, error, n, last):
        """资金账户查询回报"""
        logging.debug(data)
        logging.debug(error)
        # account = VtAccountData()
        # account.gatewayName = self.gatewayName
        #
        # # 账户代码
        # account.accountID = data['AccountID']
        # account.vtAccountID = '.'.join([self.gatewayName, account.accountID])
        #
        # # 数值相关
        # account.preBalance = data['PreBalance']
        # account.available = data['Available']
        # account.commission = data['Commission']
        # account.margin = data['CurrMargin']
        # account.closeProfit = data['CloseProfit']
        # account.positionProfit = data['PositionProfit']
        #
        # # 这里的balance和快期中的账户不确定是否一样，需要测试
        # account.balance = (data['PreBalance'] - data['PreCredit'] - data['PreMortgage'] +
        #                    data['Mortgage'] - data['Withdraw'] + data['Deposit'] +
        #                    data['CloseProfit'] + data['PositionProfit'] + data['CashIn'] -
        #                    data['Commission'])
        #
        # # 推送
        # self.gateway.onAccount(account)

    def onRspQrySettlementInfo(self, data, error, n, last):
        """查询结算信息回报"""
        logging.debug(u'查询结算信息回报')
        logging.debug(data)
        logging.debug(error)

        if data:
            Content = data.get('Content', '')
            logging.debug(Content.decode('GBK'))

            file_object = open('settlement2.txt', 'a')
            file_object.write(Content)
            file_object.close()

    ###################################################################################################################
    # 响应回调方法 没有使用的虚方法实现
    ###################################################################################################################
    def onRspOrderAction(self, data, error, n, last):
        """撤单错误（柜台）"""
        logging.debug(data)
        logging.debug(error)

        self._order_manager.update_blotter(data)

        # err = VtErrorData()
        # err.gatewayName = self.gatewayName
        # err.errorID = error['ErrorID']
        # err.errorMsg = error['ErrorMsg'].decode('gbk')
        # self.gateway.onError(err)

    def onRspUserPasswordUpdate(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspTradingAccountPasswordUpdate(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspOrderInsert(self, data, error, n, last):
        """发单错误（柜台）"""
        logging.debug(data)
        logging.debug(error)

    def onRspParkedOrderInsert(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspParkedOrderAction(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQueryMaxOrderVolume(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspSettlementInfoConfirm(self, data, error, n, last):
        """确认结算信息回报"""
        logging.debug(data)
        logging.debug(error)

    def onRspRemoveParkedOrder(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspRemoveParkedOrderAction(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspExecOrderInsert(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspExecOrderAction(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspForQuoteInsert(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQuoteInsert(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQuoteAction(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQryInvestor(self, data, error, n, last):
        """投资者查询回报"""
        logging.debug(data)
        logging.debug(error)

    def onRspQryTradingCode(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQryInstrumentMarginRate(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQryInstrumentCommissionRate(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQryExchange(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQryProduct(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQryInstrument(self, data, error, n, last):
        """合约查询回报"""
        logging.debug(data)
        logging.debug(error)

    def onRspQryDepthMarketData(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQryTransferBank(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQryInvestorPositionDetail(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQryNotice(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQrySettlementInfoConfirm(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQryInvestorPositionCombineDetail(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQryCFMMCTradingAccountKey(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQryEWarrantOffset(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQryInvestorProductGroupMargin(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQryExchangeMarginRate(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQryExchangeMarginRateAdjust(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQryExchangeRate(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQrySecAgentACIDMap(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQryOptionInstrTradeCost(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQryOptionInstrCommRate(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQryExecOrder(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQryForQuote(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQryQuote(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQryTransferSerial(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQryAccountregister(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspError(self, error, n, last):
        """错误回报"""
        logging.debug(error)

    def onErrRtnOrderInsert(self, data, error):
        """发单错误回报（交易所）"""
        error_message = error.get('ErrorMsg', '')
        logging.debug(u'收到订单委托回报(异常情况).ErrorMsg:%s.\n%s\n%s' % (error_message.decode('GBK'), data, error))

    def onErrRtnOrderAction(self, data, error):
        """撤单错误回报（交易所）"""
        logging.debug(data)
        logging.debug(error)

    def onRtnInstrumentStatus(self, data):
        """"""
        logging.debug(data)

    def onRtnTradingNotice(self, data):
        """"""
        logging.debug(data)

    def onRtnErrorConditionalOrder(self, data):
        """"""
        logging.debug(data)

    def onRtnExecOrder(self, data):
        """"""
        logging.debug(data)

    def onErrRtnExecOrderInsert(self, data, error):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onErrRtnExecOrderAction(self, data, error):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onErrRtnForQuoteInsert(self, data, error):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRtnQuote(self, data):
        """"""
        logging.debug(data)

    def onErrRtnQuoteInsert(self, data, error):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onErrRtnQuoteAction(self, data, error):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRtnForQuoteRsp(self, data):
        """"""
        logging.debug(data)

    def onRspQryContractBank(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQryParkedOrder(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQryParkedOrderAction(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQryTradingNotice(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQryBrokerTradingParams(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQryBrokerTradingAlgos(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRtnFromBankToFutureByBank(self, data):
        """"""
        logging.debug(data)

    def onRtnFromFutureToBankByBank(self, data):
        """"""
        logging.debug(data)

    def onRtnRepealFromBankToFutureByBank(self, data):
        """"""
        logging.debug(data)

    def onRtnRepealFromFutureToBankByBank(self, data):
        """"""
        logging.debug(data)

    def onRtnFromBankToFutureByFuture(self, data):
        """"""
        logging.debug(data)

    def onRtnFromFutureToBankByFuture(self, data):
        """"""
        logging.debug(data)

    def onRtnRepealFromBankToFutureByFutureManual(self, data):
        """"""
        logging.debug(data)

    def onRtnRepealFromFutureToBankByFutureManual(self, data):
        """"""
        logging.debug(data)

    def onRtnQueryBankBalanceByFuture(self, data):
        """"""
        logging.debug(data)

    def onErrRtnBankToFutureByFuture(self, data, error):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onErrRtnFutureToBankByFuture(self, data, error):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onErrRtnRepealBankToFutureByFutureManual(self, data, error):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onErrRtnRepealFutureToBankByFutureManual(self, data, error):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onErrRtnQueryBankBalanceByFuture(self, data, error):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRtnRepealFromBankToFutureByFuture(self, data):
        """"""
        logging.debug(data)

    def onRtnRepealFromFutureToBankByFuture(self, data):
        """"""
        logging.debug(data)

    def onRspFromBankToFutureByFuture(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspFromFutureToBankByFuture(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRspQueryBankAccountMoneyByFuture(self, data, error, n, last):
        """"""
        logging.debug(data)
        logging.debug(error)

    def onRtnOpenAccountByBank(self, data):
        """"""
        logging.debug(data)

    def onRtnCancelAccountByBank(self, data):
        """"""
        logging.debug(data)

    def onRtnChangeAccountByBank(self, data):
        """"""
        logging.debug(data)


def next_order_status(current_status, status_event):
    if current_status == 'pending':
        next_status = order_status_from_pending.get(status_event, '')

        if next_status:
            return next_status

    elif current_status == 'open':
        next_status = order_status_from_open.get(status_event, '')

        if next_status:
            return next_status

    return current_status


order_status_from_pending = {
    # 状态未知
    'a': '',
    'b': '',
    'c': '',
    # 全部成交
    '0': 'filled',
    # 部分成交,剩余等待成交
    '1': 'partial_filled',
    # 全部撤单
    '2': 'canceled',
    # 等待成交
    '3': 'open',
    # 等待报单
    '4': 'pending',
    # 全部撤单
    '5': 'rejected'
}

order_status_from_open = {
    # 状态未知
    'a': '',
    'b': '',
    'c': '',
    # 全部成交
    '0': 'filled',
    # 部分成交,剩余等待成交
    '1': 'partial_filled',
    # 全部撤单
    '2': 'canceled',
    # 等待成交
    '3': 'open',
    # 等待报单
    '4': 'pending',
    # 全部撤单
    '5': 'canceled'
}
