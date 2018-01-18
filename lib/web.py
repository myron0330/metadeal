# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Web interface.
# **********************************************************************************#
from flask import Flask
from flask_restful import Api
from . trader import (
    feedback_worker,
    database_worker
)


server = Flask(__name__)
api = Api(server)
feedback_worker()
database_worker()
