# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
from datetime import datetime
from lib.data.database_api import *


print(get_trading_days(datetime(2015, 1, 1), datetime(2015, 2, 1)))
print(get_direct_trading_day(datetime(2015, 1, 1), step=0, forward=True))
print(get_direct_trading_day(datetime(2015, 1, 1), step=1, forward=True))
print(get_direct_trading_day(datetime(2015, 1, 1), step=1, forward=False))
print(load_futures_base_info(['RB1810']))
print (load_futures_main_contract(contract_objects=['RB', 'AG'], start='20180401', end='20180502'))
daily_data = load_futures_daily_data(['RB1810', 'RM809'],
                                     get_trading_days('20180301', '20180401'),
                                     attributes=['closePrice', 'turnoverValue'])
print(daily_data)
base_info = load_futures_base_info(['RB1810', 'RM809'])
print(base_info)
minute_data = load_futures_minute_data(['RB1810', 'RM809'], get_trading_days('20180614', '20180616'), freq='15m')
# test_data = load_futures_rt_minute_data(['RB1810'])
print(minute_data)
