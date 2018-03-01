# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
#   Author: Myron
# **********************************************************************************#
import json
import chardet
import pandas as pd
from functools import wraps
from StringIO import StringIO
from flask import request, make_response


def get_username():
    """
    Get user name.
    """
    return request.headers.get('username', 'Anonymous')


def get_headers():
    """
    Get headers
    """
    return request.headers


def pre_process_request_data(request_data):
    """
    Pro process request database.
    """
    if request_data is None:
        request_data = dict()
    return request_data


def get_request_info():
    """
    Get request info.
    """
    info = {
        'req_method': request.method,
        'req_data': pre_process_request_data(request.json if request.data else dict()),
        'req_type': request.path.split('/')[1].split('_')[0],
        'req_args': request.args,
    }
    return info


def cross_site(func):
    """
    cross site.
    """
    @wraps(func)
    def decorator(*args, **kwargs):
        """
        decorator
        """
        ret = func(*args, **kwargs)
        code = 200
        if isinstance(ret, tuple) and len(ret) == 2:
            code, ret = ret
        response = make_response(json.dumps(ret), code)
        response.headers['Access-Control-Allow-Origin'] = request.headers.get('Origin')
        response.headers['Access-Control-Allow-Methods'] = 'PUT,GET,POST,DELETE,OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = \
            "Origin, X-Requested-With, Content-Type, Accept, Authorization"
        response.headers['Content-Type'] = 'application/json'
        response.headers['Access-Control-Allow-Credentials'] = 'true'
        return response
    return decorator


def upload_tables_data(file_name, data,
                       return_type='frame'):
    """
    Upload tables database, support excel and csv.

    Args:
         file_name(string): file name
         data(string): database
         return_type(string): type of return value
    """
    encoding = chardet.detect(data).get('encoding')
    file_data = StringIO(data)

    if file_name.endswith('.csv'):
        frame = pd.read_csv(file_data, encoding=encoding)
    elif file_name.endswith('.xlsx'):
        frame = pd.read_excel(file_data, encoding=encoding)
    else:
        raise Exception('file type is not supported.')
    if return_type == 'dict':
        return frame.to_dict()
    return frame


def download_excel_from_(frame, file_name='excel.xlsx',
                         return_type='string_io'):
    """
    Download tables database

    Args:
         frame(frame): database frame
         file_name(string): file name
         return_type(string): type of return value
    """
    file_type = file_name.split('.')[-1]
    if return_type == 'string_io':
        string_io = StringIO()
        if file_type in ['xlsx', 'xls']:
            excel_writer = pd.ExcelWriter(file_name, engine='xlsxwriter')
            excel_writer.book.filename = string_io
            frame.to_excel(excel_writer, sheet_name='Sheet1')
            excel_writer.save()
        elif file_type == 'csv':
            string_io.write(frame.to_csv())
        return string_io
