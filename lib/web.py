# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Web interface.
# **********************************************************************************#
from flask import Flask
from flask_restful import Api


server = Flask(__name__)
api = Api(server)
