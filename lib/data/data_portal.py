# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:　Data portal engine.
# **********************************************************************************#
from . asset_service import (
    AssetService,
    AssetType
)
from . base_service import ServiceInterface
from . calendar_service import CalendarService
from . market_service import MarketService
from . universe_service import UniverseService
from .. configs import logger


class DataPortal(ServiceInterface):
    """
    DataPortal: 数据引擎，用以更新数据
    """

    def __init__(self, asset_service=None,
                 calendar_service=None,
                 market_service=None,
                 universe_service=None,
                 ):
        super(DataPortal, self).__init__()
        self.asset_service = asset_service or AssetService()
        self.calendar_service = calendar_service or CalendarService()
        self.market_service = market_service or MarketService()
        self.universe_service = universe_service or UniverseService()

    def batch_load_data(self, sim_params, disable_service=None, **kwargs):
        """
        Batch load according to sim_params.
        Args:
            sim_params(obj): sim_params
            disable_service(list): disable service
            **kwargs: key-value parameters
        """
        logger.info('[DataPortal] Begin batch load data.')
        start = sim_params.start
        end = sim_params.end
        universe = sim_params.universe
        benchmark = sim_params.major_benchmark
        disable_service = disable_service or list()
        self.calendar_service.batch_load_data(start, end)
        trading_days = self.calendar_service.trading_days
        if 'universe_service' not in disable_service:
            self.universe_service.batch_load_data(universe=universe,
                                                  trading_days=trading_days,
                                                  benchmark=benchmark)
            self.universe_service.full_universe_set |= set(sim_params.position_base)
        if sim_params.accounts:
            for account, config in sim_params.accounts.iteritems():
                self.universe_service.full_universe_set |= set(config.position_base)
        full_universe = self.universe_service.full_universe
        if 'asset_service' not in disable_service:
            self.asset_service.batch_load_data(full_universe, expand_continuous_future=True)
            expanded_base_futures = self.asset_service.filter_symbols(AssetType.BASE_FUTURES)
            self.universe_service.full_universe_set |= set(expanded_base_futures)
        self.universe_service.rebuild_universe()
        if 'market_service' not in disable_service:
            self.market_service.batch_load_data(full_universe,
                                                calendar_service=self.calendar_service,
                                                asset_service=self.asset_service,
                                                universe_service=self.universe_service)
        logger.info('[DataPortal] End batch load data.')
        return self

    def subset(self, start, end, universe=None, benchmark=None, adj=None, factor_tables=None, **kwargs):
        new_portal = DataPortal()
        new_portal.calendar_service = self.calendar_service.subset(start, end, universe, **kwargs)
        start = new_portal.calendar_service.trading_days[0]
        end = new_portal.calendar_service.trading_days[-1]
        all_sub_trading_days = new_portal.calendar_service.all_trading_days
        new_portal.universe_service = self.universe_service
        full_universe = new_portal.universe_service.full_universe
        new_portal.asset_service = self.asset_service.subset(start, end, full_universe, **kwargs)
        new_portal.market_service = self.market_service.subset(all_sub_trading_days[0], end,
                                                               full_universe, adj=adj, **kwargs)
        return new_portal
