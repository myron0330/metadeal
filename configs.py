# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Global config variables.
# **********************************************************************************#
import os
import logging
import ConfigParser
from logging.config import fileConfig


current_path = os.path.dirname(__file__)
log_path = '{}/etc/log.cfg'.format(current_path)
app_path = '{}/etc/service.cfg'.format(current_path)
fileConfig(log_path)
logger = logging.getLogger("main")
config = ConfigParser.RawConfigParser()
config.read(app_path)
