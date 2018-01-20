# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
from lib.core.enums import SchemaType
from lib.core.schema import *
from lib.data.mongodb_api import dump_schema_to_mongodb, query_from_mongodb


portfolio_schema = PortfolioSchema()
# print dump_schema_to_mongodb(SchemaType.portfolio, portfolio_schema)
data = query_from_mongodb(SchemaType.portfolio, portfolio_id='e62747f0-fdad-11e7-9255-acbc32c13f9d', key=None)[0]
print data.to_dict()

