# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
import time
from lib.api.ctp import *
from lib.core.ctp import *
from lib.configs import logger
from . ctp_base import get_temp_path


def generate_request_id(func):
    """
    Decorator: Generate request id.
    Args:
        func(func): function.

    Returns:
        decorator: function decorator.
    """
    def _decorator(self, *args, **kwargs):
        self._generate_next_request_id()
        return func(self, *args, **kwargs)

    return _decorator


class CtpTraderGateway(TdApi):
    """
    CTP Trader Gateway.
    """
    def __init__(self, user_id=None, password=None, broker_id=None, address=None, 
                 request_id=0, event_engine=None):
        super(CtpTraderGateway, self).__init__()
        self.user_id = user_id
        self.password = password
        self.broker_id = broker_id
        self.address = address
        self.event_engine = event_engine

        self.request_id = request_id
        self.orderRef = None

        self.connection_status = False
        self.login_status = False
        self.auth_status = False
        self.auth_code = None
        self.user_product_info = None
        self.login_failed = False
        self.front_id = None
        self.session_id = None

        self.posDict = {}
        self.symbolExchangeDict = {}
        self.symbolSizeDict = {}

        self.requireAuthentication = False

    def __setattr__(self, attribute, value):
        """
        Add type mapping to some attributes.

        Args:
            attribute(string): attribute
            value(obj): value
        """
        type_mapping = {
            'user_id': str,
            'password': str,
            'broker_id': str,
            'address': str
        }
        if attribute in type_mapping:
            object.__setattr__(self, attribute, type_mapping[attribute](value))
        else:
            object.__setattr__(self, attribute, value)

    def connect(self, user_id=None, password=None, broker_id=None, address=None,
                auth_code=None, user_product_info=None):
        """
        Initialize connect.

        Args:
            user_id(string): user id.
            password(string): password.
            broker_id(string): broker id.
            address(string): address.
            auth_code(string): authentication code
            user_product_info(string): product info

        Returns:

        """
        self.user_id = user_id or self.user_id
        self.password = password or self.password
        self.broker_id = broker_id or self.broker_id
        self.address = address or self.address
        self.auth_code = auth_code or self.auth_code
        self.user_product_info = user_product_info or self.user_product_info
        logger.info('[connect] user_id: {}, broker_id: {}, address: {}'.format(
            self.user_id, self.broker_id, self.address))

        if not self.connection_status:
            path = get_temp_path(self.__class__.__name__ + '_')
            self.createFtdcTraderApi(path)

            # 设置数据同步模式为推送从今日开始所有数据
            # need set 1 when trading.
            self.subscribePrivateTopic(0)
            self.subscribePublicTopic(0)

            self.registerFront(self.address)
            self.init()
        else:
            if self.requireAuthentication and not self.auth_status:
                self.authenticate()
            elif not self.login_status:
                self.login()
        time.sleep(0.1)

    def login(self):
        """
        Request for logging in.
        """
        if self.login_failed:
            return

        if self.user_id and self.password and self.broker_id:
            logger.info('[login] user_id: {},'
                        'broker_id: {}, '
                        'address: {}'.format(self.user_id, self.broker_id, self.address))
            request = {
                'UserID': self.user_id,
                'Password': self.password,
                'BrokerID': self.broker_id
            }
            self.reqUserLogin(request, self._generate_next_request_id())
            time.sleep(0.1)
            self.settlement_confirm()
            time.sleep(0.1)

    def settlement_confirm(self):
        """
        Request for settlement confirming.
        """
        if self.user_id and self.password and self.broker_id:
            logger.info('[settlement_confirm] user_id: {},'
                        'broker_id: {}, '
                        'address: {}'.format(self.user_id, self.broker_id, self.address))
            request = {
                'UserID': self.user_id,
                'Password': self.password,
                'BrokerID': self.broker_id
            }
            self.reqSettlementInfoConfirm(request, self._generate_next_request_id())
            time.sleep(0.1)

    def authenticate(self):
        """
        Request for authenticate.
        """
        if self.user_id and self.broker_id and self.auth_code and self.user_product_info:
            logger.info('[authenticate] user_id: {},'
                        'broker_id: {}, '
                        'auth_code: {},'
                        'user_product_info'.format(self.user_id, self.broker_id,
                                                   self.auth_code, self.user_product_info))
            request = {
                'UserID': self.user_id,
                'BrokerID': self.broker_id,
                'AuthCode': self.auth_code,
                'UserProductInfo': self.user_product_info
            }
            self.request_id += 1
            self.reqAuthenticate(request, self.request_id)
            time.sleep(0.1)

    def onFrontConnected(self):
        """
        Server connected.
        """
        self.connection_status = True
        if self.requireAuthentication:
            self.authenticate()
        else:
            self.login()

    def onFrontDisconnected(self, n):
        """
        Server disconnected.
        """
        self.connection_status = False
        self.login_status = False
        logger.debug('Disconnected with the CTP front server.[n:%s]' % n)

    def onRspUserLogin(self, data, error, n, last):
        """
        Login response, deal with user login.

        Args:
            data(dict): response data
            error(dict): error data
            n(unused): unused
            last(unused): unused
        """
        if error['ErrorID'] == 0:
            logger.info('[onRspUserLogin] succeed. '
                        'front_id: {}, session_id: {}'.format(str(data['FrontID']), str(data['SessionID'])))
            self.front_id = str(data['FrontID'])
            self.session_id = str(data['SessionID'])
            self.login_status = True
            self.request_id += 1
        else:
            # 标识登录失败，防止用错误信息连续重复登录
            self.login_failed = True

    def onRspUserLogout(self, data, error, n, last):
        """
        Logout response, deal with user login.

        Args:
            data(dict): response data
            error(dict): error data
            n(unused): unused
            last(unused): unused
        """
        if error['ErrorID'] == 0:
            self.login_status = False

    def onRspAuthenticate(self, data, error, n, last):
        """
        Authentication response, deal with authentication.

        Args:
            data(dict): response data
            error(dict): error data
            n(unused): unused
            last(unused): unused
        """
        if error['ErrorID'] == 0:
            self.front_id = str(data['FrontID'])
            self.session_id = str(data['SessionID'])
            self.auth_status = True
            self.login()
        else:
            self.auth_status = False

    @generate_request_id
    def query_account(self):
        """
        Query the basic information of account.
        """
        logger.info('[query_account] broker_id: {}, user_id: {}.'
                    ''.format(str(self.broker_id), str(self.user_id)))
        request = {
            'BrokerID': self.broker_id,
            'InvestorID': self.user_id
        }
        self.reqQryTradingAccount(request, self.request_id)
        time.sleep(0.1)

    def onRspQryTradingAccount(self, data, error, n, last):
        """
        Response of the basic information of account.

        Args:
            data(dict): response data
            error(dict): error data
            n(unused): unused
            last(unused): unused
        """
        response = AccountResponse.from_ctp(data)
        logger.info('[onRspQryTradingAccount] {}'.format(response))

    @generate_request_id
    def query_positions(self):
        """
        Query positions information.
        """
        logger.info('[query_positions] broker_id: {}, user_id: {}.'
                    ''.format(str(self.broker_id), str(self.user_id)))
        request = {
            'BrokerID': self.broker_id,
            'InvestorID': self.user_id,
        }
        self.reqQryInvestorPosition(request, self.request_id)
        time.sleep(0.1)

    @generate_request_id
    def query_position_detail(self):
        """
        Query positions detail information.
        """
        logger.info('[query_positions_detail] broker_id: {}, user_id: {}.'
                    ''.format(str(self.broker_id), str(self.user_id)))
        request = {
            'BrokerID': self.broker_id,
            'InvestorID': self.user_id,
        }
        self.reqQryInvestorPositionDetail(request, self.request_id)
        time.sleep(0.1)

    def _generate_next_request_id(self):
        """
        Get next request id.
        """
        self.request_id += 1
        return self.request_id

    def onRtnTrade(self, data):
        """
        Trade response information.

        Args:
            data(dict): response data.
        """
        response = TradeResponse.from_ctp(data)
        logger.info('[onRtnTrade] {}'.format(response))

    def onRspOrderInsert(self, data, error, n, last):
        """发单错误（柜台）"""
        # 推送委托信息
        # order = VtOrderData()
        # order.gatewayName = self.gatewayName
        # order.symbol = data['InstrumentID']
        # order.exchange = exchangeMapReverse[data['ExchangeID']]
        # order.vtSymbol = order.symbol
        # order.orderID = data['OrderRef']
        # order.vtOrderID = '.'.join([self.gatewayName, order.orderID])
        # order.direction = directionMapReverse.get(data['Direction'], DIRECTION_UNKNOWN)
        # order.offset = offsetMapReverse.get(data['CombOffsetFlag'], OFFSET_UNKNOWN)
        # order.status = STATUS_REJECTED
        # order.price = data['LimitPrice']
        # order.totalVolume = data['VolumeTotalOriginal']
        # self.gateway.onOrder(order)
        #
        # # 推送错误信息
        # err = VtErrorData()
        # err.gatewayName = self.gatewayName
        # err.errorID = error['ErrorID']
        # err.errorMsg = error['ErrorMsg'].decode('gbk')
        # self.gateway.onError(err)
        raise NotImplementedError

    def onRspParkedOrderInsert(self, data, error, n, last):
        """"""
        pass

    def onRspParkedOrderAction(self, data, error, n, last):
        """"""
        pass

    def onRspOrderAction(self, data, error, n, last):
        """撤单错误（柜台）"""
        # err = VtErrorData()
        # err.gatewayName = self.gatewayName
        # err.errorID = error['ErrorID']
        # err.errorMsg = error['ErrorMsg'].decode('gbk')
        # self.gateway.onError(err)
        raise NotImplementedError

    def onRspQueryMaxOrderVolume(self, data, error, n, last):
        """"""
        pass

    def onRspSettlementInfoConfirm(self, data, error, n, last):
        """确认结算信息回报"""
        print data
        # self.writeLog(text.SETTLEMENT_INFO_CONFIRMED)
        #
        # # 查询合约代码
        # self.request_id += 1
        # self.reqQryInstrument({}, self.request_id)

    def onRspRemoveParkedOrder(self, data, error, n, last):
        """"""
        pass

    def onRspRemoveParkedOrderAction(self, data, error, n, last):
        """"""
        pass

    def onRspExecOrderInsert(self, data, error, n, last):
        """"""
        pass

    def onRspExecOrderAction(self, data, error, n, last):
        """"""
        pass

    def onRspForQuoteInsert(self, data, error, n, last):
        """"""
        pass

    def onRspQuoteInsert(self, data, error, n, last):
        """"""
        pass

    def onRspQuoteAction(self, data, error, n, last):
        """"""
        pass

    def onRspLockInsert(self, data, error, n, last):
        """"""
        pass

    def onRspCombActionInsert(self, data, error, n, last):
        """"""
        pass

    def onRspQryOrder(self, data, error, n, last):
        """"""
        pass

    def onRspQryTrade(self, data, error, n, last):
        """"""
        pass

    def onRspQryInvestorPosition(self, data, error, n, last):
        """持仓查询回报"""
        if not data['InstrumentID']:
            return

        # 获取持仓缓存对象
        posName = '.'.join([data['InstrumentID'], data['PosiDirection']])
        if posName in self.posDict:
            pos = self.posDict[posName]
        else:
            pos = VtPositionData()
            self.posDict[posName] = pos

            pos.gatewayName = self.gatewayName
            pos.symbol = data['InstrumentID']
            pos.vtSymbol = pos.symbol
            pos.direction = posiDirectionMapReverse.get(data['PosiDirection'], '')
            pos.vtPositionName = '.'.join([pos.vtSymbol, pos.direction])

            # 针对上期所持仓的今昨分条返回（有昨仓、无今仓），读取昨仓数据
        if data['YdPosition'] and not data['TodayPosition']:
            pos.ydPosition = data['Position']

        # 计算成本
        size = self.symbolSizeDict[pos.symbol]
        cost = pos.price * pos.position * size

        # 汇总总仓
        pos.position += data['Position']
        pos.positionProfit += data['PositionProfit']

        # 计算持仓均价
        if pos.position and size:
            pos.price = (cost + data['PositionCost']) / (pos.position * size)

        # 读取冻结
        if pos.direction is DIRECTION_LONG:
            pos.frozen += data['LongFrozen']
        else:
            pos.frozen += data['ShortFrozen']

        # 查询回报结束
        if last:
            # 遍历推送
            for pos in self.posDict.values():
                self.gateway.onPosition(pos)

            # 清空缓存
            self.posDict.clear()
    #
    # def onRspQryTradingAccount(self, data, error, n, last):
    #     """资金账户查询回报"""
    #     account = VtAccountData()
    #     account.gatewayName = self.gatewayName
    #
    #     # 账户代码
    #     account.accountID = data['AccountID']
    #     account.vtAccountID = '.'.join([self.gatewayName, account.accountID])
    #
    #     # 数值相关
    #     account.preBalance = data['PreBalance']
    #     account.available = data['Available']
    #     account.commission = data['Commission']
    #     account.margin = data['CurrMargin']
    #     account.closeProfit = data['CloseProfit']
    #     account.positionProfit = data['PositionProfit']
    #
    #     # 这里的balance和快期中的账户不确定是否一样，需要测试
    #     account.balance = (data['PreBalance'] - data['PreCredit'] - data['PreMortgage'] +
    #                        data['Mortgage'] - data['Withdraw'] + data['Deposit'] +
    #                        data['CloseProfit'] + data['PositionProfit'] + data['CashIn'] -
    #                        data['Commission'])
    #
    #     # 推送
    #     self.gateway.onAccount(account)

    def onRspQryInvestor(self, data, error, n, last):
        """"""
        pass

    def onRspQryTradingCode(self, data, error, n, last):
        """"""
        pass

    def onRspQryInstrumentMarginRate(self, data, error, n, last):
        """"""
        pass

    def onRspQryInstrumentCommissionRate(self, data, error, n, last):
        """"""
        pass

    def onRspQryExchange(self, data, error, n, last):
        """"""
        pass

    def onRspQryProduct(self, data, error, n, last):
        """"""
        pass

    def onRspQryInstrument(self, data, error, n, last):
        """合约查询回报"""
        contract = VtContractData()
        contract.gatewayName = self.gatewayName

        contract.symbol = data['InstrumentID']
        contract.exchange = exchangeMapReverse[data['ExchangeID']]
        contract.vtSymbol = contract.symbol  # '.'.join([contract.symbol, contract.exchange])
        contract.name = data['InstrumentName'].decode('GBK')

        # 合约数值
        contract.size = data['VolumeMultiple']
        contract.priceTick = data['PriceTick']
        contract.strikePrice = data['StrikePrice']
        contract.productClass = productClassMapReverse.get(data['ProductClass'], PRODUCT_UNKNOWN)
        contract.expiryDate = data['ExpireDate']

        # ETF期权的标的命名方式需要调整（ETF代码 + 到期月份）
        if contract.exchange in [EXCHANGE_SSE, EXCHANGE_SZSE]:
            contract.underlyingSymbol = '-'.join([data['UnderlyingInstrID'], str(data['ExpireDate'])[2:-2]])
        # 商品期权无需调整
        else:
            contract.underlyingSymbol = data['UnderlyingInstrID']

            # 期权类型
        if contract.productClass is PRODUCT_OPTION:
            if data['OptionsType'] == '1':
                contract.optionType = OPTION_CALL
            elif data['OptionsType'] == '2':
                contract.optionType = OPTION_PUT

        # 缓存代码和交易所的印射关系
        self.symbolExchangeDict[contract.symbol] = contract.exchange
        self.symbolSizeDict[contract.symbol] = contract.size

        # 推送
        self.gateway.onContract(contract)

        # 缓存合约代码和交易所映射
        symbolExchangeDict[contract.symbol] = contract.exchange

        if last:
            self.writeLog(text.CONTRACT_DATA_RECEIVED)

    def onRspQryDepthMarketData(self, data, error, n, last):
        """"""
        pass

    def onRspQrySettlementInfo(self, data, error, n, last):
        """"""
        pass

    def onRspQryTransferBank(self, data, error, n, last):
        """"""
        pass

    def onRspQryInvestorPositionDetail(self, data, error, n, last):
        """"""
        logger.info(data)
        pass

    def onRspQryNotice(self, data, error, n, last):
        """"""
        pass

    def onRspQrySettlementInfoConfirm(self, data, error, n, last):
        """"""
        pass

    def onRspQryInvestorPositionCombineDetail(self, data, error, n, last):
        """"""
        pass

    def onRspQryCFMMCTradingAccountKey(self, data, error, n, last):
        """"""
        pass

    def onRspQryEWarrantOffset(self, data, error, n, last):
        """"""
        pass

    def onRspQryInvestorProductGroupMargin(self, data, error, n, last):
        """"""
        pass

    def onRspQryExchangeMarginRate(self, data, error, n, last):
        """"""
        pass

    def onRspQryExchangeMarginRateAdjust(self, data, error, n, last):
        """"""
        pass

    def onRspQryExchangeRate(self, data, error, n, last):
        """"""
        pass

    def onRspQrySecAgentACIDMap(self, data, error, n, last):
        """"""
        pass

    def onRspQryProductExchRate(self, data, error, n, last):
        """"""
        pass

    def onRspQryProductGroup(self, data, error, n, last):
        """"""
        pass

    def onRspQryOptionInstrTradeCost(self, data, error, n, last):
        """"""
        pass

    def onRspQryOptionInstrCommRate(self, data, error, n, last):
        """"""
        pass

    def onRspQryExecOrder(self, data, error, n, last):
        """"""
        pass

    def onRspQryForQuote(self, data, error, n, last):
        """"""
        pass

    def onRspQryQuote(self, data, error, n, last):
        """"""
        pass

    def onRspQryLock(self, data, error, n, last):
        """"""
        pass

    def onRspQryLockPosition(self, data, error, n, last):
        """"""
        pass

    def onRspQryInvestorLevel(self, data, error, n, last):
        """"""
        pass

    def onRspQryExecFreeze(self, data, error, n, last):
        """"""
        pass

    def onRspQryCombInstrumentGuard(self, data, error, n, last):
        """"""
        pass

    def onRspQryCombAction(self, data, error, n, last):
        """"""
        pass

    def onRspQryTransferSerial(self, data, error, n, last):
        """"""
        pass

    def onRspQryAccountregister(self, data, error, n, last):
        """"""
        pass

    def onRspError(self, error, n, last):
        """错误回报"""
        err = VtErrorData()
        err.gatewayName = self.gatewayName
        err.errorID = error['ErrorID']
        err.errorMsg = error['ErrorMsg'].decode('gbk')
        self.gateway.onError(err)

    def onRtnOrder(self, data):
        """报单回报"""
        # 更新最大报单编号
        # newref = data['OrderRef']
        # self.orderRef = max(self.orderRef, int(newref))
        #
        # # 创建报单数据对象
        # order = VtOrderData()
        # order.gatewayName = self.gatewayName
        #
        # # 保存代码和报单号
        # order.symbol = data['InstrumentID']
        # order.exchange = exchangeMapReverse[data['ExchangeID']]
        # order.vtSymbol = order.symbol  # '.'.join([order.symbol, order.exchange])
        #
        # order.orderID = data['OrderRef']
        # # CTP的报单号一致性维护需要基于frontID, sessionID, orderID三个字段
        # # 但在本接口设计中，已经考虑了CTP的OrderRef的自增性，避免重复
        # # 唯一可能出现OrderRef重复的情况是多处登录并在非常接近的时间内（几乎同时发单）
        # # 考虑到VtTrader的应用场景，认为以上情况不会构成问题
        # order.vtOrderID = '.'.join([self.gatewayName, order.orderID])
        #
        # order.direction = directionMapReverse.get(data['Direction'], DIRECTION_UNKNOWN)
        # order.offset = offsetMapReverse.get(data['CombOffsetFlag'], OFFSET_UNKNOWN)
        # order.status = statusMapReverse.get(data['OrderStatus'], STATUS_UNKNOWN)
        #
        # # 价格、报单量等数值
        # order.price = data['LimitPrice']
        # order.totalVolume = data['VolumeTotalOriginal']
        # order.tradedVolume = data['VolumeTraded']
        # order.orderTime = data['InsertTime']
        # order.cancelTime = data['CancelTime']
        # order.frontID = data['FrontID']
        # order.sessionID = data['SessionID']
        # # 推送
        # self.gateway.onOrder(order)
        pass

    def onErrRtnOrderInsert(self, data, error):
        """发单错误回报（交易所）"""
        # 推送委托信息
        # order = VtOrderData()
        # order.gatewayName = self.gatewayName
        # order.symbol = data['InstrumentID']
        # order.exchange = exchangeMapReverse[data['ExchangeID']]
        # order.vtSymbol = order.symbol
        # order.orderID = data['OrderRef']
        # order.vtOrderID = '.'.join([self.gatewayName, order.orderID])
        # order.direction = directionMapReverse.get(data['Direction'], DIRECTION_UNKNOWN)
        # order.offset = offsetMapReverse.get(data['CombOffsetFlag'], OFFSET_UNKNOWN)
        # order.status = STATUS_REJECTED
        # order.price = data['LimitPrice']
        # order.totalVolume = data['VolumeTotalOriginal']
        # self.gateway.onOrder(order)
        #
        # # 推送错误信息
        # err = VtErrorData()
        # err.gatewayName = self.gatewayName
        # err.errorID = error['ErrorID']
        # err.errorMsg = error['ErrorMsg'].decode('gbk')
        # self.gateway.onError(err)
        raise NotImplementedError

    def onErrRtnOrderAction(self, data, error):
        """撤单错误回报（交易所）"""
        # err = VtErrorData()
        # err.gatewayName = self.gatewayName
        # err.errorID = error['ErrorID']
        # err.errorMsg = error['ErrorMsg'].decode('gbk')
        # self.gateway.onError(err)
        raise NotImplementedError

    def onRtnInstrumentStatus(self, data):
        """"""
        # logger.info(data)
        pass

    def onRtnTradingNotice(self, data):
        """"""
        pass

    def onRtnErrorConditionalOrder(self, data):
        """"""
        pass

    def onRtnExecOrder(self, data):
        """"""
        pass

    def onErrRtnExecOrderInsert(self, data, error):
        """"""
        pass

    def onErrRtnExecOrderAction(self, data, error):
        """"""
        pass

    def onErrRtnForQuoteInsert(self, data, error):
        """"""
        pass

    def onRtnQuote(self, data):
        """"""
        pass

    def onErrRtnQuoteInsert(self, data, error):
        """"""
        pass

    def onErrRtnQuoteAction(self, data, error):
        """"""
        pass

    def onRtnForQuoteRsp(self, data):
        """"""
        pass

    def onRtnCFMMCTradingAccountToken(self, data):
        """"""
        pass

    def onRtnLock(self, data):
        """"""
        pass

    def onErrRtnLockInsert(self, data, error):
        """"""
        pass

    def onRtnCombAction(self, data):
        """"""
        pass

    def onErrRtnCombActionInsert(self, data, error):
        """"""
        pass

    def onRspQryContractBank(self, data, error, n, last):
        """"""
        pass

    def onRspQryParkedOrder(self, data, error, n, last):
        """"""
        pass

    def onRspQryParkedOrderAction(self, data, error, n, last):
        """"""
        pass

    def onRspQryTradingNotice(self, data, error, n, last):
        """"""
        pass

    def onRspQryBrokerTradingParams(self, data, error, n, last):
        """"""
        pass

    def onRspQryBrokerTradingAlgos(self, data, error, n, last):
        """"""
        pass

    def onRspQueryCFMMCTradingAccountToken(self, data, error, n, last):
        """"""
        pass

    def onRtnFromBankToFutureByBank(self, data):
        """"""
        pass

    def onRtnFromFutureToBankByBank(self, data):
        """"""
        pass

    def onRtnRepealFromBankToFutureByBank(self, data):
        """"""
        pass

    def onRtnRepealFromFutureToBankByBank(self, data):
        """"""
        pass

    def onRtnFromBankToFutureByFuture(self, data):
        """"""
        pass

    def onRtnFromFutureToBankByFuture(self, data):
        """"""
        pass

    def onRtnRepealFromBankToFutureByFutureManual(self, data):
        """"""
        pass

    def onRtnRepealFromFutureToBankByFutureManual(self, data):
        """"""
        pass

    def onRtnQueryBankBalanceByFuture(self, data):
        """"""
        pass

    def onErrRtnBankToFutureByFuture(self, data, error):
        """"""
        pass

    def onErrRtnFutureToBankByFuture(self, data, error):
        """"""
        pass

    def onErrRtnRepealBankToFutureByFutureManual(self, data, error):
        """"""
        pass

    def onErrRtnRepealFutureToBankByFutureManual(self, data, error):
        """"""
        pass

    def onErrRtnQueryBankBalanceByFuture(self, data, error):
        """"""
        pass

    def onRtnRepealFromBankToFutureByFuture(self, data):
        """"""
        pass

    def onRtnRepealFromFutureToBankByFuture(self, data):
        """"""
        pass

    def onRspFromBankToFutureByFuture(self, data, error, n, last):
        """"""
        pass

    def onRspFromFutureToBankByFuture(self, data, error, n, last):
        """"""
        pass

    def onRspQueryBankAccountMoneyByFuture(self, data, error, n, last):
        """"""
        pass

    def onRtnOpenAccountByBank(self, data):
        """"""
        pass

    def onRtnCancelAccountByBank(self, data):
        """"""
        pass

    def onRtnChangeAccountByBank(self, data):
        """"""
        pass

    def qryAccount(self):
        """查询账户"""
        self.request_id += 1
        self.reqQryTradingAccount({}, self.request_id)

    def sendOrder(self, orderReq):
        """发单"""
        self.request_id += 1
        self.orderRef += 1

        req = {}

        req['InstrumentID'] = orderReq.symbol
        req['LimitPrice'] = orderReq.price
        req['VolumeTotalOriginal'] = orderReq.volume

        # 下面如果由于传入的类型本接口不支持，则会返回空字符串
        req['OrderPriceType'] = priceTypeMap.get(orderReq.priceType, '')
        req['Direction'] = directionMap.get(orderReq.direction, '')
        req['CombOffsetFlag'] = offsetMap.get(orderReq.offset, '')

        req['OrderRef'] = str(self.orderRef)
        req['InvestorID'] = self.user_id
        req['UserID'] = self.user_id
        req['BrokerID'] = self.broker_id

        req['CombHedgeFlag'] = defineDict['THOST_FTDC_HF_Speculation']  # 投机单
        req['ContingentCondition'] = defineDict['THOST_FTDC_CC_Immediately']  # 立即发单
        req['ForceCloseReason'] = defineDict['THOST_FTDC_FCC_NotForceClose']  # 非强平
        req['IsAutoSuspend'] = 0  # 非自动挂起
        req['TimeCondition'] = defineDict['THOST_FTDC_TC_GFD']  # 今日有效
        req['VolumeCondition'] = defineDict['THOST_FTDC_VC_AV']  # 任意成交量
        req['MinVolume'] = 1  # 最小成交量为1

        # 判断FAK和FOK
        if orderReq.priceType == PRICETYPE_FAK:
            req['OrderPriceType'] = defineDict["THOST_FTDC_OPT_LimitPrice"]
            req['TimeCondition'] = defineDict['THOST_FTDC_TC_IOC']
            req['VolumeCondition'] = defineDict['THOST_FTDC_VC_AV']
        if orderReq.priceType == PRICETYPE_FOK:
            req['OrderPriceType'] = defineDict["THOST_FTDC_OPT_LimitPrice"]
            req['TimeCondition'] = defineDict['THOST_FTDC_TC_IOC']
            req['VolumeCondition'] = defineDict['THOST_FTDC_VC_CV']

        self.reqOrderInsert(req, self.request_id)

        # 返回订单号（字符串），便于某些算法进行动态管理
        vtOrderID = '.'.join([self.gatewayName, str(self.orderRef)])
        return vtOrderID

    def cancelOrder(self, cancelOrderReq):
        """撤单"""
        self.request_id += 1

        req = {}

        req['InstrumentID'] = cancelOrderReq.symbol
        req['ExchangeID'] = cancelOrderReq.exchange
        req['OrderRef'] = cancelOrderReq.orderID
        req['FrontID'] = cancelOrderReq.frontID
        req['SessionID'] = cancelOrderReq.sessionID

        req['ActionFlag'] = defineDict['THOST_FTDC_AF_Delete']
        req['BrokerID'] = self.broker_id
        req['InvestorID'] = self.user_id

        self.reqOrderAction(req, self.request_id)

    def close(self):
        """关闭"""
        self.exit()

    def writeLog(self, content):
        """发出日志"""
        log = VtLogData()
        log.gatewayName = self.gatewayName
        log.logContent = content
        self.gateway.onLog(log)





