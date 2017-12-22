# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: error utils
# **********************************************************************************#


error = (lambda code, message: {'code': code, 'data': message})


class SchemaException(Exception):
    """
    Schema exception.
    """
    pass


class Errors(object):

    INVALID_SCHEMA_ID_INPUT = SchemaException(error(500, '[SchemaException] INVALID Schema ID INPUT.'))

    @classmethod
    def enumerates(cls):
        return [value for attr, value in cls.__dict__.iteritems()]

    @classmethod
    def error_types(cls):
        return tuple([Exception, ImportError, NotImplementedError,
                      SchemaException])
