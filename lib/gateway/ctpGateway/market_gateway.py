# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
import os
import time
from lib.api.ctp import *
from lib.configs import logger
from lib.core.ctp import *
from lib.event.event_base import *
from . ctp_base import get_temp_path


class CTPMarketGateway(MdApi):
    """
    CTP Market Gateway.
    """
    def __init__(self, user_id=None, password=None, broker_id=None, address=None, event_engine=None):
        super(CTPMarketGateway, self).__init__()
        self.user_id = user_id
        self.password = password
        self.broker_id = broker_id
        self.address = address
        self.event_engine = event_engine

        self.connection_status = False  # 连接状态
        self.login_status = False  # 登录状态
        self.login_failed = False
        self.subscribed_symbols = set()  # 已订阅合约代码
        self.request_id = 0
        self.front_id = None
        self.tickTime = None  # 最新行情time对象
        self.auth_code = None
        self.auth_status = False
        self.user_product_info = None
        self.session_id = None

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
        Request for logging in.
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

    def onRtnDepthMarketData(self, data):
        """
        Market data quotation response.

        Args:
            data(dict): market data.
        """
        tick_data = Tick.from_ctp(data)
        parameters = {
            'tick': tick_data
        }
        self.event_engine.publish(EventType.event_on_tick, **parameters)
