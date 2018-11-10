# -*- coding: utf-8 -*-
import datetime
from copy import copy
from collections import deque, defaultdict
from utils.linked_list import (
    LinkedList, Node
)
from utils.error import Errors
from . calendar_service import CalendarService


def is_collection(item):
    """
    判断item是否为list, tuple, 或者set
    Args:
        item: 传入待判断的symbol或symbol集合

    Returns:
        boolean
    """
    return isinstance(item, (list, tuple, set))


class UniverseService(object):
    """
    Universe service.
    """

    def __init__(self, universe=None, trading_days=None, benchmarks=list(), init_universe_list=list()):
        self.universe = universe
        self.trading_days = trading_days
        self.benchmarks = set(benchmarks) if is_collection(benchmarks) else {benchmarks}
        self.init_universe = set(init_universe_list) if is_collection(init_universe_list) else {init_universe_list}
        self.dynamic_universe_dict = None
        self.dynamic_universe_ranked = None
        self.full_universe_set = set(benchmarks) | set(init_universe_list)
        self._universe_to_remove = set()
        self.l1_ban_list = None
        self.l4_ban_list = None
        self.calendar_service = None
        self.industry_resource = None

        # symbol dynamic dict
        self.symbol_collection_dict = {}
        self.untradable_dict = {}
        self.st_dict = {}
        self.cached_factor_data = {}

    def batch_load_data(self, universe=None, trading_days=None, benchmark=None, **kwargs):
        """
        回测服务场景下预先“全量”load数据

        Args:
            universe(Universe):
            trading_days(list):
            benchmark(string): benchmark
            kwargs
        """
        self.benchmarks = {benchmark} if benchmark else self.benchmarks
        self.universe = universe or self.universe
        self.trading_days = trading_days or self.trading_days
        start, end = self.trading_days[0], self.trading_days[-1]
        calendar_service = kwargs.get('calendar_service')
        if not calendar_service:
            self.calendar_service = CalendarService().batch_load_data(start, end)
        composite_target_symbols = self.universe.composites.recursive(
            formula=(lambda x, y: x | y), formatter=(lambda x: set(x.obj.symbol_collection)))
        target_statics = composite_target_symbols - set(self.symbol_collection_dict)
        composite_static_collection = self.universe.composites.recursive(
            formula=(lambda x, y: x | y), formatter=(lambda x: x.obj.static_collection))
        self.universe.static_collection = composite_static_collection | target_statics
        for symbol_data in self.symbol_collection_dict.itervalues():
            for daily_collection in symbol_data.itervalues():
                self.full_universe_set |= daily_collection
        self.full_universe_set |= target_statics
        self.full_universe_set |= composite_static_collection
        if self.benchmarks:
            self.full_universe_set |= self.benchmarks

    def subset(self, *args, **kwargs):
        """
        Subset universe service.
        """
        return self

    def rebuild_universe(self, cached_factor_data=None):
        assert isinstance(self.universe, Universe)
        previous_trading_day_map = \
            {
                trading_day: self.calendar_service.previous_trading_day_map[trading_day].strftime('%Y-%m-%d')
                for trading_day in self.trading_days
            }
        data = {
            'all_static_universe': self.universe.static_collection,
            'symbol_collection_dict': self.symbol_collection_dict,
            'previous_trading_day_map': previous_trading_day_map,
            # 'stock_list_by_day': self.stock_list_by_day,
            'untradable_dict': self.untradable_dict,
            'industry_dict': {k: v for k, v in self.symbol_collection_dict.iteritems() if isinstance(k, IndBase)},
            'st_dict': self.st_dict,
            'cached_factor_data': cached_factor_data
        }
        self.universe.load_data_from_service(self.trading_days, from_subset_data=data)
        self.universe.build()
        # summarize UniverseService level attribute
        self.dynamic_universe_dict = self.universe.dynamic_universe
        self.dynamic_universe_ranked = self.universe.dynamic_universe_ranked
        if self.benchmarks:
            self.full_universe_set |= self.benchmarks
        # 必要的attribute清空
        self._clearing_universe_service_and_node()
        return self

    def _clearing_universe_service_and_node(self):
        """
        Build后清理subset分发的数据
        """
        self.universe.cached_factor_data = None
        self.universe.st_dict = None
        self.universe.industry_dict = None
        # self.universe.stock_list_by_day = None
        self.universe.symbol_collection_dict = None
        self.cached_factor_data = None
        # self.stock_list_by_day = None
        self.industry_dict = None
        self.symbol_collection_dict = None

    def remove(self, symbols):
        symbols_set = set(symbols)
        self._universe_to_remove |= symbols_set
        self.full_universe_set -= symbols_set
        for date, univ in self.dynamic_universe_dict.iteritems():
            self.dynamic_universe_dict[date] = univ - symbols_set
        for date, univ in self.dynamic_universe_ranked.iteritems():
            self.dynamic_universe_ranked[date] = univ - symbols_set

    def add_init_universe(self, init_universe):
        """
        在universe_full中增加额外的股票，让这部分股票的行情也能够被预先加载。init_universe中的股票不会出现在account.current_universe中
        场景：
            - 回测时universe之外的security_base
            - 以及模拟交易过程
        """
        self.init_universe |= set(init_universe)
        self.full_universe_set |= self.init_universe

    def view(self, current_date=None, remove_halt=False, st_level='ignore', with_benchmark=False,
             with_init_universe=False, ban_level=None, subset=None, mergeset=None, apply_sort=False,
             position_securities=[]):
        """
        查看特定情形下的universe内容
        Args:
            current_date(datetime): 查阅universe的时间，如果为空则为trading_days期间所有设计个股的总集合
            remove_halt(boolean): 是否去除停牌
            st_level(str): option: 'st', 'no_st', 'ignore', default 'no_st'
            with_benchmark(boolean): 是否考虑benchmark
            with_init_universe: 是否考虑用户初始股票池
            ban_level(str): 合规检查力度，'l0'表示不做合规检查，'l1'表示做Level1的合规检查，'l4'表示做Level4的合规检查
            position_securities(list of str): 当前持仓的股票池
            说明：处理顺序为 current_date -> with_benchmark -> with_init_value -> ban_invalid -> remove_halt -> remove_st
        Returns:
            list: 根据要求获取到的股票列表
        Example:
            >> universe_full = universe_service.view()
            >> current_universe_2016_1_5 = universe_service.view(current_date=datetime(2016, 1, 5), with_benchmark=True)
            >> universe_2016_1_5 = universe_service.view(current_date=datetime.datetime(2016, 1, 5), with_benchmark=True)
        """
        universe_result = copy(self.full_universe_set)
        if mergeset is not None:
            universe_result |= set(mergeset)
        if subset is not None:
            universe_result &= set(subset)
        if current_date is not None:
            assert isinstance(current_date, datetime.datetime), Errors.INVALID_CURRENT_DAY
            universe_result &= self.dynamic_universe_dict[current_date]
        if len(position_securities) > 0:
            universe_result |= set(position_securities)
        if with_benchmark:
            universe_result |= self.benchmarks
        if with_init_universe:
            universe_result |= self.init_universe
        if remove_halt:
            universe_result -= self.untradable_dict[current_date]
        if st_level == 'st':
            universe_result &= self.st_dict[current_date]
        elif st_level == 'no_st':
            universe_result -= self.st_dict[current_date]
        if self.l1_ban_list is not None and (ban_level == 'l1' or ban_level == 'l4'):
            universe_result -= self.l1_ban_list
        if self.l4_ban_list is not None and ban_level == 'l4':
            universe_result -= self.l4_ban_list
        if apply_sort:
            universe_result = [e for e in self.dynamic_universe_ranked[current_date] if e in universe_result]
        return universe_result

    @property
    def full_universe(self):
        return list(self.view(with_benchmark=True, with_init_universe=True))


class BuilderType(object):

    """
    构建类型枚举
    """
    APPLY_FILTER = 'apply_filter'
    CAN_TRADE = 'can_trade'
    IS_ST = 'is_st'
    # IS_NEW = 'is_new'
    EXCHANGE = 'exchange'
    INDUSTRY = 'industry'
    SECTOR = 'sector'
    APPLY_SORT = 'apply_sort'
    EXCLUDE_LIST = 'exclude_list'
    CUSTOM_UNIVERSE = 'custom_universe'


class Universe(object):

    """
    提供Universe符号计算逻辑。

    Example:
        >> Universe('HS300') + Universe('ZZ500') + Universe('IF') #定义由沪深300，中证500和沪深300主力合约构成的股票池
        >> Universe(IndSW.JiSuanJiL1).filter_universe(Factor.PE.nsmall(100))
            + StockScreener(Factor.PE.nsmall(100), Universe(IndSW.ChuanMeiL1)) #获取计算机和传媒行业PE最低的10只股票组成的股票池
        说明： StockScreener(factor_filter_condition, universe)和universe.filter_universe(factor_filter_condition)效果是一样的
    """

    # NODE_SYMBOLS = 'SYMBOLS'
    # NODE_ADD = 'ADD'
    # NODE_FILTER = 'FILTER'

    def __init__(self, *target_symbols, **kwargs):
        self.trading_days = list()
        self.builder = deque()
        self.composites = LinkedList(*[Node(self)]*2)
        self._dynamic_universe = {}
        self.symbol_collection = set(target_symbols)
        static_universe = kwargs.get('static_universe')
        self.static_collection = set(static_universe) if static_universe else set()
        self.full_universe = set()
        self._previous_trading_day_map = dict()
        self.symbol_data = defaultdict(dict)
        self.static_universe = defaultdict(set)
        self.dynamic_universe = defaultdict(set)
        self.dynamic_universe_ranked = []
        self.cached_factor_data = {}
        self.st_dict = defaultdict(set)
        self.industry_dict = defaultdict(set)
        self.untradable_dict = defaultdict(set)

    def __add__(self, other):
        if isinstance(other, (str, unicode)):
            other = Universe(other)
        elif isinstance(other, (list, set, tuple)):
            other = Universe(static_universe=other)
        elif isinstance(other, Universe):
            pass
        else:
            raise Exception
        new_universe = Universe()
        new_universe.composites = self.composites + other.composites
        return new_universe

    def __radd__(self, other):
        return self.__add__(other)

    def apply_filter(self, filter_condition, skip_halt=True):
        self.builder.append((BuilderType.APPLY_FILTER, (filter_condition, skip_halt)))
        return self

    def is_st(self, formula):
        self.builder.append((BuilderType.IS_ST, formula))
        return self

    def can_trade(self, formula):
        self.builder.append((BuilderType.CAN_TRADE, formula))
        return self

    def exchange(self, formula):
        self.builder.append((BuilderType.EXCHANGE, formula))
        return self

    def exclude_list(self, formula):
        formula = formula.split(',') if isinstance(formula, basestring) else formula
        self.builder.append((BuilderType.EXCLUDE_LIST, formula))
        return self

    def custom_universe(self, formula):
        formula = formula.split(',') if isinstance(formula, basestring) else formula
        self.builder.append((BuilderType.CUSTOM_UNIVERSE, formula))
        return self

    def apply_sort(self, filter_condition):
        self.builder.append((BuilderType.APPLY_SORT, filter_condition))
        return self

    def _load_from_subset(self, subset_data):
        """
        Universe接收通过UniverseService传入的数据

        Args:
            subset_data: dict of universe service data
        """
        self.symbol_data['static'] = {e: e for e in subset_data['all_static_universe']}
        self.symbol_data['dynamic'].update(subset_data['symbol_collection_dict'])
        self._previous_trading_day_map = subset_data['previous_trading_day_map']
        # self._cached_trading_days = subset_data['_cached_trading_days']
        # self._cached_trading_days_index = subset_data['_cached_trading_days_index']
        # self.stock_list_by_day = subset_data['stock_list_by_day']
        self.untradable_dict = subset_data['untradable_dict']
        self.industry_dict = subset_data['industry_dict']
        self.st_dict = subset_data['st_dict']
        self.cached_factor_data = subset_data['cached_factor_data']

    def load_data_from_service(self, trading_days, from_subset_data=None):
        """
        加载 universe 数据，包括 symbol 数据、辅助性数据

        Args:
            trading_days(list of datetime.datetime): 交易日期
            from_subset_data(dict): load universe所需数据来自传入的subset
        """
        if set(trading_days) <= set(self.trading_days):
            return
        self.trading_days = trading_days
        self.composites.traversal(func=dispatch_trading_days, trading_days=trading_days)
        self._load_from_subset(from_subset_data)
        self.composites.traversal(func=dispatch_symbol_data, data=self.symbol_data)
        self.composites.traversal(func=dispatch_auxiliary_data, head=self)

    def build(self):
        """
        构建筛选结果
        """
        def func(node):
            node.obj.pipeline()
        self.composites.traversal(func=func)
        self.static_universe = self.composites.recursive(formula=(lambda x, y: operate_or(x, y)),
                                                         formatter=(lambda x: x.obj.static_universe))
        self.dynamic_universe = self.composites.recursive(formula=(lambda x, y: operate_or(x, y)),
                                                          formatter=(lambda x: x.obj.dynamic_universe))
        self.full_universe = self.composites.recursive(formula=(lambda x, y: operate_or(x, y)),
                                                       formatter=(lambda x: x.obj.full_universe))
        head = self.composites.link_head.obj

    def _expand_custom_universe(self, custom_universe):
        """
        将custom_universe展开，当custom_universe为按日期动态定义时

        Returns: custom_universe

        """
        if custom_universe and isinstance(custom_universe, dict):
            last_universe = set()
            for date in self.trading_days:
                if date not in custom_universe:
                    custom_universe.update({date: last_universe})
                else:
                    custom_universe[date] = set(custom_universe[date])
                    last_universe = custom_universe[date]
        return custom_universe

    def pipeline(self):
        """
        构建个性化 Universe 对象
        """
        builders = filter(lambda x: x[0] != BuilderType.APPLY_SORT, self.builder)
        while builders:
            builder_type, formula = builders.pop(0)
            if builder_type == BuilderType.CUSTOM_UNIVERSE:
                formula = self._expand_custom_universe(formula)
                if formula and len(self.symbol_collection) != 0:
                    self.dynamic_universe = operate_and(self.dynamic_universe, formula)
                    self.static_universe = operate_and(self.static_universe, formula)
                else:
                    self.dynamic_universe = operate_or(self.dynamic_universe, formula)
                    self.static_universe = operate_or(self.static_universe, formula)
            elif builder_type == BuilderType.EXCLUDE_LIST:
                exclude_list = set(formula)
                if exclude_list:
                    self.dynamic_universe = operate_minus(self.dynamic_universe, exclude_list)
                    self.static_universe = operate_minus(self.static_universe, exclude_list)
            elif builder_type == BuilderType.IS_ST:
                if formula is True:
                    self.dynamic_universe = operate_and(target_a=self.dynamic_universe, target_b=self.st_dict)
                    self.static_universe = operate_and(target_a=self.static_universe, target_b=self.st_dict)
                else:
                    self.dynamic_universe = operate_minus(target_a=self.dynamic_universe, target_b=self.st_dict)
                    self.static_universe = operate_minus(target_a=self.static_universe, target_b=self.st_dict)
            elif builder_type == BuilderType.CAN_TRADE:
                if formula is True:
                    self.dynamic_universe = operate_minus(target_a=self.dynamic_universe, target_b=self.untradable_dict)
                    self.static_universe = operate_minus(target_a=self.static_universe, target_b=self.untradable_dict)
        self.full_universe = set()
        for dynamic_universe in self.dynamic_universe.itervalues():
            self.full_universe |= dynamic_universe


def dispatch_symbol_data(node, data):
    """
    对链表中每个 Node 分配加载的数据

    Args:
        node(Node): Universe 节点
        data(dict): 加载的数据
    """
    universe_obj = node.obj
    static_universe = universe_obj.symbol_collection & set(data['static']) | universe_obj.static_collection
    dynamic_data = {key: value for key, value in data['dynamic'].iteritems()
                    if key in universe_obj.symbol_collection}

    for date in universe_obj.trading_days:
        universe_obj.static_universe[date] |= static_universe
        universe_obj.dynamic_universe[date] |= static_universe
        universe_obj.full_universe |= universe_obj.dynamic_universe[date]

    for dynamic_universe_dict in dynamic_data.itervalues():
        for date, dynamic_universe in dynamic_universe_dict.iteritems():
            universe_obj.dynamic_universe[date] |= dynamic_universe | universe_obj.static_universe[date]
            universe_obj.full_universe |= universe_obj.dynamic_universe[date]


def dispatch_auxiliary_data(node, head):
    """
    对链表中每个 Node 分配辅助的数据

    Args:
        node(Node): Universe 节点
        head(head): 头节点
    """
    universe_obj = node.obj
    universe_obj._previous_trading_day_map = head._previous_trading_day_map
    universe_obj.st_dict = head.st_dict
    universe_obj.untradable_dict = head.untradable_dict
    universe_obj.industry_dict = head.industry_dict
    universe_obj.cached_factor_data = head.cached_factor_data
    # universe_obj.stock_list_by_day = head.stock_list_by_day
    # new_dict在pipeline的时候生成
    # universe_obj.new_dict = head.new_dict


def dispatch_trading_days(node, trading_days):
    """
    对链表中每个 Node 分配已加载的交易日

    Args:
        node(Node): Universe 节点
        trading_days(list of datetime.datetime): 交易日列表
    """
    universe_obj = node.obj
    universe_obj.trading_days = trading_days


def operate_and(target_a, target_b):
    """
    集合与运算

    Args:
        target_a(object): 集合 a
        target_b(object): 集合 b
    """
    assert isinstance(target_a, (dict, set)), Errors.INVALID_OPERATOR_INPUT
    assert isinstance(target_b, (dict, set)), Errors.INVALID_OPERATOR_INPUT
    if isinstance(target_a, set) and isinstance(target_b, set):
        return target_a & target_b
    elif isinstance(target_a, dict) and isinstance(target_b, set):
        for date, value in target_a.iteritems():
            value &= target_b
        return target_a
    elif isinstance(target_a, set) and isinstance(target_b, dict):
        for date, value in target_b.iteritems():
            value &= target_a
        return target_b
    elif isinstance(target_a, dict) and isinstance(target_b, dict):
        for date, value in target_a.iteritems():
            value &= target_b.get(date)
        return target_a


def operate_or(target_a, target_b):
    """
    集合或运算

    Args:
        target_a(object): 集合 a
        target_b(object): 集合 b
    """
    assert isinstance(target_a, (dict, set)), Errors.INVALID_OPERATOR_INPUT
    assert isinstance(target_b, (dict, set)), Errors.INVALID_OPERATOR_INPUT
    if isinstance(target_a, set) and isinstance(target_b, set):
        return target_a | target_b
    elif isinstance(target_a, dict) and isinstance(target_b, set):
        for date, value in target_a.iteritems():
            value |= target_b
        return target_a
    elif isinstance(target_a, set) and isinstance(target_b, dict):
        for date, value in target_b.iteritems():
            value |= target_a
        return target_b
    elif isinstance(target_a, dict) and isinstance(target_b, dict):
        for date, value in target_a.iteritems():
            value |= target_b[date]
        return target_a


def operate_minus(target_a, target_b):
    """
    集合非（减）运算

    Args:
        target_a(object): 集合 a
        target_b(object): 集合 b
    """
    assert isinstance(target_a, (dict, set)), Errors.INVALID_OPERATOR_INPUT
    assert isinstance(target_b, (dict, set)), Errors.INVALID_OPERATOR_INPUT
    if isinstance(target_a, set) and isinstance(target_b, set):
        return target_a - target_b
    elif isinstance(target_a, dict) and isinstance(target_b, set):
        for date, value in target_a.iteritems():
            value -= target_b
        return target_a
    elif isinstance(target_a, dict) and isinstance(target_b, dict):
        for date, value in target_a.iteritems():
            value -= target_b[date]
        return target_a


def filter_endswith(target, end_letter):
    """
    集合过滤满足endswith

    Args:
        target(object): 集合
        end_letter(str): 结尾字母
    """
    assert isinstance(target, (dict, set)), Errors.INVALID_OPERATOR_INPUT
    assert isinstance(end_letter, str), Errors.INVALID_OPERATOR_INPUT
    for date, value in target.iteritems():
        target.update({date: set(e for e in value if e.endswith(end_letter))})
    return target
