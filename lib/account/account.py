# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Account manager File
#   Author: Myron
# **********************************************************************************#
from copy import copy
from utils.error_utils import Errors
from . import (
    FuturesAccount
)
from .. core.enums import SecuritiesType
from .. data import (
    AssetType,
    get_future_code
)
from .. trade import (
    Commission,
    Slippage,
)
from .. const import DEFAULT_ACCOUNT_NAME


class AccountConfig(object):

    __slots__ = [
        'account_type',
        'capital_base',
        'commission',
        'slippage',
        'margin_rate',
        'position_base',
        'cost_base',
        'dividend_method'
    ]

    def __init__(self, account_type, capital_base=None, commission=None,
                 slippage=None, margin_rate=None, position_base=None, cost_base=None, dividend_method=None):
        self.account_type = account_type
        self.capital_base = capital_base
        self.commission = commission
        self.slippage = slippage
        self.margin_rate = margin_rate if margin_rate else dict()
        self.position_base = position_base if position_base else dict()
        self.cost_base = cost_base if cost_base else dict()
        self.dividend_method = dividend_method

    @property
    def __dict__(self):
        return {key: self.__getattribute__(key) for key in self.__slots__}

    def to_dict(self):
        """
        To dict
        """
        return self.__dict__

    def __repr__(self):
        return 'Account(account_type: {}, capital_base: {}, commission: {}, slippage: {}, margin_rate: {}, ' \
               'amount_base: {}, cost_base: {}, dividend_method: {})'.format(self.account_type, self.capital_base,
                                                                             self.commission, self.slippage,
                                                                             self.margin_rate, self.position_base,
                                                                             self.cost_base, self.dividend_method)


class AccountManager(object):

    ACCOUNT_TYPE_MAPPING = {
        'futures': FuturesAccount,
    }

    def __init__(self, accounts=None):
        self._accounts = accounts if accounts else dict()
        self._registered_accounts = dict()
        self._registered_accounts_params = dict()

    @classmethod
    def from_config(cls, clock, sim_params, data_portal, accounts=None):
        """
        Generate account manager from config.

        Args:
            clock(obj): Clock
            sim_params(obj): simulation parameters
            data_portal(obj): data portal
            accounts(dict): accounts config dict

        Returns:
            AccountManager(obj): account manager
        """
        accounts = accounts or sim_params.accounts
        if not accounts:
            accounts = {DEFAULT_ACCOUNT_NAME: AccountConfig(
                SecuritiesType.futures, 
                capital_base=sim_params.capital_base,
                commission=sim_params.commission,
                slippage=sim_params.slippage,
                margin_rate=sim_params.margin_rate,
                position_base=sim_params.position_base,
                cost_base=sim_params.cost_base)}
        account_manager = cls(accounts)
        account_manager.register_accounts(clock, sim_params, data_portal)
        return account_manager

    @property
    def accounts(self):
        """
        Accounts config dict
        """
        return self._accounts

    @property
    def registered_accounts(self):
        """
        Registered accounts dict
        """
        return self._registered_accounts

    @property
    def registered_accounts_params(self):
        """
        Registered accounts parameters dict
        """
        return self._registered_accounts_params

    @property
    def compatible_account(self):
        """
        获取兼容接口用的默认账户，如果使用了Quartz3的写法定义账户，则随机获取一个股票账户作为兼容接口账户
        """
        if DEFAULT_ACCOUNT_NAME in self._registered_accounts:
            return self._registered_accounts[DEFAULT_ACCOUNT_NAME]
        else:
            accounts = self.filter_accounts(SecuritiesType.futures).values()
            return accounts[0] if len(accounts) > 0 else None

    def register_accounts(self, clock, sim_params, data_portal, accounts=None):
        """
        Register accounts by clock, sim_params and data_portal.

        Args:
            clock(obj): Clock
            sim_params(obj): simulation parameters
            data_portal(obj): data portal
            accounts(dict): accounts config
        """
        self._accounts.update(accounts) if accounts else None
        for account, config in self._accounts.iteritems():
            current_sim_params = copy(sim_params)
            current_sim_params.capital_base = self.initiate_capital_base(config, current_sim_params)
            current_sim_params.cash = current_sim_params.capital_base
            current_sim_params.commission = self.initiate_commission(config, data_portal)
            current_sim_params.slippage = self.initiate_slippage(config)
            current_sim_params.margin_rate = self.initiate_margin_rate(config, data_portal)

            if config.position_base is None:
                config.position_base = current_sim_params.position_base
            if config.cost_base is None:
                config.cost_base = current_sim_params.cost_base
            custom_position_base = current_sim_params.position_base_by_accounts.get(account)
            custom_cost_base = current_sim_params.cost_base_by_accounts.get(account)
            if custom_position_base is not None:
                config.position_base = custom_position_base
            if custom_cost_base is not None:
                config.cost_base = custom_cost_base

            current_sim_params.position_base = config.position_base
            current_sim_params.cost_base = config.cost_base
            current_sim_params.portfolio = self.initiate_portfolio(config, current_sim_params)
            if config.account_type == SecuritiesType.futures:
                self._registered_accounts[account] = \
                    FuturesAccount.from_config(clock, current_sim_params, data_portal)
            else:
                raise Errors.INVALID_ACCOUNT_TYPE

            config.capital_base = current_sim_params.capital_base
            config.commission = current_sim_params.commission
            config.slippage = current_sim_params.slippage
            config.margin_rate = current_sim_params.margin_rate
            self._registered_accounts_params[account] = current_sim_params

    def filter_accounts(self, account_type):
        """
        Filter accounts by account_type

        Args:
            account_type(string): account type

        """
        account_type = account_type.split(',') if isinstance(account_type, basestring) else account_type
        assert isinstance(account_type, list)
        account_types = tuple(map(lambda x: self.ACCOUNT_TYPE_MAPPING[x], account_type))
        return {
            account_name: account for account_name, account in self.registered_accounts.iteritems()
            if isinstance(account, account_types)
        }

    def get_account(self, account_name):
        """
        Get account by account_name

        Args:
            account_name(string): account name
        """
        if account_name in self.registered_accounts:
            return self.registered_accounts[account_name]
        else:
            raise Errors.INVALID_ACCOUNT_NAME

    @staticmethod
    def initiate_commission(account_config, data_portal):
        """
        Initialize commission object.

        Args:
            account_config(AccountConfig): account config
            data_portal(obj): DataPortal
        """
        if account_config.account_type == SecuritiesType.futures:

            def _normalize_commission(commission, full_universe):
                """
                Normalize commission by full universe.

                Args:
                    commission(obj): user input commission
                    full_universe(iterable): full universe list

                Returns:
                    dict: normalized commission.
                """
                normalized_commission = dict()
                if isinstance(commission, dict):
                    if isinstance(commission.values()[0], tuple):
                        if len(commission.values()[0]) == 2:
                            normalized_commission = {get_future_code(symbol): Commission(
                                commission[symbol][0], commission[symbol][0], commission[symbol][1])
                                for symbol in commission.iterkeys()}
                        elif len(commission.values()[0]) == 3:
                            normalized_commission = {get_future_code(symbol): Commission(
                                commission[symbol][0], commission[symbol][1], commission[symbol][2])
                                for symbol in commission.iterkeys()}
                        else:
                            raise Errors.INVALID_COMMISSION
                else:
                    normalized_commission = {get_future_code(symbol): commission for symbol in full_universe}
                return normalized_commission

            universe_set = data_portal.universe_service.full_universe
            target_universe = set(data_portal.asset_service.filter_symbols(AssetType.FUTURES, universe_set))
            return _normalize_commission(account_config.commission, target_universe)
        else:
            return account_config.commission or Commission()

    @staticmethod
    def initiate_slippage(account_config):
        """
        Initialize slippage object.

        Args:
            account_config(AccountConfig): account config
        """
        return account_config.slippage if account_config.slippage else Slippage()

    @staticmethod
    def initiate_margin_rate(account_config, data_portal):
        """
        Initialize commission object.

        Args:
            account_config(AccountConfig): account config
            data_portal(obj): DataPortal
        """

        if account_config.account_type == SecuritiesType.futures:

            def _normalize_margin_rate(margin_rate, full_universe):
                """
                Normalize margin_rate by full universe.

                Args:
                    margin_rate(obj): user input commission
                    full_universe(iterable): full universe list

                Returns:
                    dict: normalized margin_rate.
                """
                normalized_margin_rate = margin_rate if isinstance(margin_rate, dict) else \
                    {get_future_code(symbol): margin_rate for symbol in full_universe}
                return normalized_margin_rate

            universe_set = data_portal.universe_service.full_universe
            target_universe = set(data_portal.asset_service.filter_symbols(AssetType.FUTURES, universe_set))
            return _normalize_margin_rate(account_config.margin_rate, target_universe)
        else:
            return account_config.margin_rate

    @staticmethod
    def initiate_portfolio(account_config, sim_params):
        """
        Initialize portfolio

        Args:
            account_config(AccountConfig): account config
            sim_params(SimulationParameters): simulation parameters
        """
        assert sim_params, Errors.INVALID_SIM_PARAMS
        if account_config.account_type == 'futures':
            return {
                'position_base': account_config.position_base,
                'cost_base': account_config.cost_base
            }
        else:
            raise Errors.INVALID_ACCOUNT_TYPE

    @staticmethod
    def initiate_capital_base(account_config, sim_param):
        """
        Initialize capital base

        Args:
            account_config(AccountConfig): account config
            sim_param(SimulationParameters): simulation parameters
        """
        if account_config.capital_base is not None:
            return account_config.capital_base
        return sim_param.capital_base
