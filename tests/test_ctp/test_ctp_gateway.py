# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
import os
import json
from lib.api.ctp import *


data = json.load(open('CTP_connect.json', 'r+'))
print data


def getTempPath(name):
    """获取存放临时文件的路径"""
    tempPath = os.path.join(os.getcwd(), 'temp')
    if not os.path.exists(tempPath):
        os.makedirs(tempPath)

    path = os.path.join(tempPath, name)
    return path


class CTPMarketGateway(MdApi):
    """CTP行情API实现"""

    # ----------------------------------------------------------------------
    def __init__(self, user_id=None, password=None, broker_id=None, address=None):
        """
        Initialize CTPMarketGateway.
        """
        super(CTPMarketGateway, self).__init__()

        self.connectionStatus = False  # 连接状态
        self.loginStatus = False  # 登录状态
        self.subscribedSymbols = set()  # 已订阅合约代码
        self.user_id = user_id
        self.password = password
        self.broker_id = broker_id
        self.address = address

        self.tickTime = None  # 最新行情time对象

    def connect(self, user_id=None, password=None, broker_id=None, address=None):
        """
        Initialize connect.

        Args:
            user_id(string): user id.
            password(string): password.
            broker_id(string): broker id.
            address(string): address.
        """
        self.user_id = user_id or self.user_id
        self.password = password or self.password
        self.broker_id = broker_id or self.broker_id
        self.address = address or self.address

        if not self.connectionStatus:
            # 创建C++环境中的API对象，这里传入的参数是需要用来保存.con文件的文件夹路径
            path = getTempPath('CTPMarketGateway' + '_')
            self.createFtdcMdApi(path)
            self.registerFront(self.address)
            # 初始化连接，成功会调用onFrontConnected
            self.init()
        # 若已经连接但尚未登录，则进行登录
        else:
            if not self.loginStatus:
                self.login()

    def subscribe(self, subscribeReq):
        """订阅合约"""
        # 这里的设计是，如果尚未登录就调用了订阅方法
        # 则先保存订阅请求，登录完成后会自动订阅
        if self.loginStatus:
            self.subscribeMarketData(str(subscribeReq.symbol))
        self.subscribedSymbols.add(subscribeReq)

    def login(self):
        """登录"""
        # 如果填入了用户名密码等，则登录
        if self.user_id and self.password and self.broker_id:
            req = dict()
            req['UserID'] = self.user_id
            req['Password'] = self.password
            req['BrokerID'] = self.broker_id
            self.reqID += 1
            self.reqUserLogin(req, self.reqID)

    def authenticate(self):
        """申请验证"""
        if self.user_id and self.broker_id and self.authCode and self.userProductInfo:
            req = dict()
            req['UserID'] = self.user_id
            req['BrokerID'] = self.broker_id
            req['AuthCode'] = self.authCode
            req['UserProductInfo'] = self.userProductInfo
            self.reqID +=1
            self.reqAuthenticate(req, self.reqID)


market_gateway = CTPMarketGateway()
address = data['mdAddress']
user_id = data['userID']
password = data['password']
broker_id = data['brokerID']
address = str(address)
market_gateway.connect(user_id=user_id, password=password, broker_id=broker_id, address=address)
market_gateway.authenticate()
