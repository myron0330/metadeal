# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
import os
import time
from lib.api.ctp import *
from lib.configs import logger


def get_temp_path(file_name):
    """
    Get path for saving temporary files.

    Args:
        file_name(string): file name
    """
    temp_path = os.path.join(os.getcwd(), 'temp')
    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    path = os.path.join(temp_path, file_name)
    return path


class CTPMarketGateway(MdApi):
    """
    CTP Market Gateway.
    """

    def __init__(self, user_id=None, password=None, broker_id=None, address=None):
        """
        Initialize CTPMarketGateway.
        """
        super(CTPMarketGateway, self).__init__()
        self.user_id = user_id
        self.password = password
        self.broker_id = broker_id
        self.address = address

        self.connection_status = False  # 连接状态
        self.login_status = False  # 登录状态
        self.login_failed = False
        self.subscribed_symbols = set()  # 已订阅合约代码
        self.request_id = 0
        self.front_id = None
        self.tickTime = None  # 最新行情time对象
        self.auth_code = None
        self.user_product_info = None
        self.session_id = None

    def connect(self, user_id=None, password=None, broker_id=None, address=None):
        """
        Initialize connect.

        Args:
            user_id(string): user id.
            password(string): password.
            broker_id(string): broker id.
            address(string): address.
        """
        logger.info('[connect] user_id: {}, broker_id: {}, address: {}'.format(user_id, broker_id, address))
        self.user_id = user_id or self.user_id
        self.password = password or self.password
        self.broker_id = broker_id or self.broker_id
        self.address = address or self.address
        if not self.connection_status:
            path = get_temp_path(self.__class__.__name__ + '_')
            self.createFtdcMdApi(path)
            self.registerFront(self.address)
            # 初始化连接，成功会调用onFrontConnected
            self.init()
        # 若已经连接但尚未登录，则进行登录
        else:
            if not self.login_status:
                self.login()
        time.sleep(0.1)

    def subscribe(self, symbols):
        """
        Subscribe market data.

        Args:
            symbols(string or list): subscribe symbol.
        """
        logger.info('[subscribe] symbols: {}'.format(symbols))
        symbols = [symbols] if isinstance(symbols, basestring) else symbols
        if self.login_status:
            for symbol in symbols:
                self.subscribeMarketData(str(symbol))
        self.subscribed_symbols |= set(symbols)
        time.sleep(0.1)

    def login(self):
        """
        Request for log in.
        """
        if self.login_failed:
            return
        if self.user_id and self.password and self.broker_id:
            logger.info('[login] user_id: {},'
                        'broker_id: {}, '
                        'address: {}'.format(self.user_id, self.broker_id, self.address))
            request = dict()
            request['UserID'] = self.user_id
            request['Password'] = self.password
            request['BrokerID'] = self.broker_id
            self.reqUserLogin(request, self.request_id)

    def authenticate(self):
        """
        Request for authenticate.
        """
        if self.user_id and self.broker_id and self.auth_code and self.user_product_info:
            logger.info('[authenticate] user_id: {},'
                        'broker_id: {}, '
                        'auth_code: {},'
                        'user_product_info'.format(self.user_id, self.broker_id, self.auth_code, self.user_product_info))
            req = dict()
            req['UserID'] = self.user_id
            req['BrokerID'] = self.broker_id
            req['AuthCode'] = self.auth_code
            req['UserProductInfo'] = self.user_product_info
            self.request_id += 1
            self.reqAuthenticate(req, self.request_id)

    def onFrontConnected(self):
        """
        Connect response, deal with front server connection.
        """
        logger.info('[onFrontConnected] connection status = True.')
        self.connection_status = True
        self.login()

    def onRspUserLogin(self, data, error, n, last):
        """
        Login response, deal with user login.

        Args:
            data(dict): response data
            error(dict): error data
            n(unused): unused
            last(unused): unused
        """
        # 如果登录成功，推送日志信息
        if error['ErrorID'] == 0:
            logger.info('[onRspUserLogin] succeed. '
                        'front_id: {}, session_id: {}'.format(str(data['FrontID']), str(data['SessionID'])))
            self.front_id = str(data['FrontID'])
            self.session_id = str(data['SessionID'])
            self.login_status = True
            # 确认结算信息
            req = dict()
            req['BrokerID'] = self.broker_id
            req['InvestorID'] = self.user_id
            self.request_id += 1
            # confirm settlement information.
            # self.reqSettlementInfoConfirm(req, self.request_id)
            # 否则，推送错误信息
        else:
            # 标识登录失败，防止用错误信息连续重复登录
            self.login_failed = True

    def onRtnDepthMarketData(self, data):
        """
        Market data quotation.

        Args:
            data(dict): market data.
        """
        print data
        # # 过滤尚未获取合约交易所时的行情推送
        # symbol = data['InstrumentID']
        # if symbol not in symbolExchangeDict:
        #     return
        #
        # # 创建对象
        # tick = VtTickData()
        # tick.gatewayName = self.gatewayName
        #
        # tick.symbol = symbol
        # tick.exchange = symbolExchangeDict[tick.symbol]
        # tick.vtSymbol = tick.symbol  # '.'.join([tick.symbol, tick.exchange])
        #
        # tick.lastPrice = data['LastPrice']
        # tick.volume = data['Volume']
        # tick.openInterest = data['OpenInterest']
        # tick.time = '.'.join([data['UpdateTime'], str(data['UpdateMillisec'] / 100)])
        #
        # # 上期所和郑商所可以直接使用，大商所需要转换
        # tick.date = data['ActionDay']
        #
        # tick.openPrice = data['OpenPrice']
        # tick.highPrice = data['HighestPrice']
        # tick.lowPrice = data['LowestPrice']
        # tick.preClosePrice = data['PreClosePrice']
        #
        # tick.upperLimit = data['UpperLimitPrice']
        # tick.lowerLimit = data['LowerLimitPrice']
        #
        # # CTP只有一档行情
        # tick.bidPrice1 = data['BidPrice1']
        # tick.bidVolume1 = data['BidVolume1']
        # tick.askPrice1 = data['AskPrice1']
        # tick.askVolume1 = data['AskVolume1']
        #
        # # 大商所日期转换
        # if tick.exchange is EXCHANGE_DCE:
        #     newTime = datetime.strptime(tick.time, '%H:%M:%S.%f').time()  # 最新tick时间戳
        #
        #     # 如果新tick的时间小于夜盘分隔，且上一个tick的时间大于夜盘分隔，则意味着越过了12点
        #     if (self.tickTime and
        #                 newTime < NIGHT_TRADING and
        #                 self.tickTime > NIGHT_TRADING):
        #         self.tradingDt += timedelta(1)  # 日期加1
        #         self.tradingDate = self.tradingDt.strftime('%Y%m%d')  # 生成新的日期字符串
        #
        #     tick.date = self.tradingDate  # 使用本地维护的日期
        #
        #     self.tickTime = newTime  # 更新上一个tick时间
        #
        # self.gateway.onTick(tick)
        #
