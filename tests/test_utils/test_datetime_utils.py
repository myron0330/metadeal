# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
from unittest import TestCase
from utils.datetime_utils import *
from datetime import datetime


class TestDatetimeUtils(TestCase):

    def test_get_clearing_date_of(self):

        print datetime.now(), get_clearing_date_of()
        now = datetime(2018, 6, 24)
        print now, get_clearing_date_of(now)
        now = datetime(2018, 6, 22, 20, 1)
        print now, get_clearing_date_of(now)
        now = datetime(2018, 6, 22, 19, 1)
        print now, get_clearing_date_of(now)
