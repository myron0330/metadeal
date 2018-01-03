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
logging.config.fileConfig(log_path)
logger = logging.getLogger("main")


################################################################
# redis client
################################################################
redis_host = config.get('redis', 'host')
redis_port = int(config.get('redis', 'port'))


################################################################
# API config
################################################################
api_token = config.get('api_client', 'token')
