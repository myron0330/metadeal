# -*- coding: utf-8 -*-

"""
fake_account.py

fake account class for docstring

@author: yudi.wu
"""


class FakeAccount(object):
    def __init__(self):
        """初始化虚拟账户"""

        return

    @property
    def current_date(self):
        """当前回测日期，格式为datetime"""

        return

    @property
    def previous_date(self):
        """当前回测日期的前一交易日，格式为datetime"""

        return

    @property
    def current_minute(self):
        """当前分钟线，格式为形如'MM:SS'的字符串"""

        return

    @property
    def current_price(self, symbol):
        """symbol最新参考价格"""

        return

    @property
    def now(self):
        """当前回测时间，格式为 datetime """

        return

    def history(self, symbol='all', time_range=1, attribute='closePrice', freq='d', **options):
        """
        历史数据统一获取接口，融合了get_history/get_attribute_history/get_symbol_history以及因子获取的功能

        Args:
            symbol (str/list/'all'): 需要获取行情的证券列表，可以为一只或者多支股票，'all'表示当前universe和benchmark所有证券。
            time_range(int): 历史数据的窗口长度。
            attribute: 数据变量名，可使用的attribute字段为: preClosePrice(仅日线), openPrice, highPrice, lowPrice, closePrice, turnoverVol, turnoverValue, adjFactor(仅日线)。
                * 如果在initialize中定义了signal_generator，那么signal_generator中使用过的预定义因子均可在history中获取到，变量名即为信号原本名称
            freq('d'/'m'): 数据获取的频度，'d'表示日线，'m'表示分钟线。'm'仅可在回测选项freq='m'时使用。
            style('ast'/'sat'/'tas'): 返回样式，默认为'ast'。
                - 'ast': key: attribute, column: symbol, index: trade_date
                - 'sat': key: symbol, column: attribute, index: trade_date
                - 'tas': key: trade_date, column: attribute, index: symbol
        Returns:
            dict of DataFrame: dict的主键为style选项中的key，值为DataFrame（column为style选项中的column，index为style选项中的index）
        """
        return

    def get_universe(self, asset_type, exclude_halt=False):
        """
        在handle_data(context)中使用，获取对应的 universe 的合约列表

        Args:
            asset_type (str or list of str): 资产类型或类型列表，可用选项为:
                                            'stock' : 股票列表
                                            'index' : 指数成分股列表
                                            'fund' : 基金列表
                                            'futures': 期货合约列表
                                            'base_futures': 普通期货合约列表
                                            'continuous_futures': 连续期货合约列表
            exclude_halt (boolean): optional, 是否去除当日停牌股票

        Returns:
            list: 返回所选择的合约列表
        """
        return

    def get_symbol(self, symbol):
        """
        在handle_data(context)中使用，获取期货连续合约在当前时间点对应的实际合约

        Args:
            symbol (str): 期货连续合约代码
        Returns:
            str: 返回连续合约对应的实际合约
        """
        return

    def mapping_changed(self, symbol):
        """
        判断context当下symbol是否存在人工合约切换

        Args:
            symbol (str): 期货连续合约代码
        """
        return

    def get_rolling_tuple(self, symbol):
        """
        返回连续合约symbol, 在回测期间所对应的具体合约切换信息。无切换时返回交易日所对应具体合约。

        Args:
            symbol (str): 期货连续合约代码
        Returns:
            (tuple), 如('IF1701', 'IF1702')
        """
        return

    def get_account(self, account):
        """
        在handle_data(context)中使用，获取账户名称所对应的账户

        Args:
            account (str): 账户名称
        Returns:
            Account: 账户对象
        """
        return

    def transfer_cash(self, origin, target, amount):
        """
        在handle_data(context)中使用，账户间转帐

        Args:
            origin (str or Account): 待转出账户名称或对象
            target (str or Account): 待转入账户名称或对象
            amount (int or float): 转帐金额
        """
        pass
