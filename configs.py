# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Global config variables.
# **********************************************************************************#
import os
import ConfigParser
import logging.config


current_path = os.path.dirname(__file__)
log_path = '{}/etc/log.cfg'.format(current_path)
app_path = '{}/etc/service.cfg'.format(current_path)
config = ConfigParser.RawConfigParser()
config.read(app_path)


################################################################
# logger
################################################################
try:
    logging.config.fileConfig(log_path)
    logger = logging.getLogger("main")
except IOError, _:
    pass


################################################################
# redis client
################################################################
redis_host = config.get('redis', 'host')
redis_port = int(config.get('redis', 'port'))


################################################################
# mongodb client
################################################################
mongodb_host = config.get('mongodb', 'host')
mongodb_port = int(config.get('mongodb', 'port'))
mongodb_path = config.get('mongodb', 'db_path')
mongodb_url = ':'.join([mongodb_host, str(mongodb_port)])
mongodb_db_name = config.get('mongodb', 'db_name')
mongodb_authenticate = config.get('mongodb', 'authenticate') == '1'
mongodb_username = config.get('mongodb', 'username')
mongodb_password = config.get('mongodb', 'password')


################################################################
# API config
################################################################
api_token = config.get('api_client', 'token')


################################################################
# Workers
################################################################
feedback_worker_enable = config.get('workers', 'feedback_worker_enable')
database_worker_enable = config.get('workers', 'database_worker_enable')


################################################################
# Working time
################################################################
futures_pre_trading_task_time = config.get('working_time', 'futures_pre_trading_task_time')
futures_post_trading_task_time = config.get('working_time', 'futures_post_trading_task_time')
futures_market_close_time = config.get('working_time', 'futures_market_close_time')


__all__ = [
    'logger',
    'redis_host',
    'redis_port',
    'mongodb_host',
    'mongodb_port',
    'mongodb_path',
    'api_token',
    'feedback_worker_enable',
    'database_worker_enable',
    'futures_pre_trading_task_time',
    'futures_post_trading_task_time',
    'futures_market_close_time'
]
