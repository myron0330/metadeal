# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File:
# **********************************************************************************#
import os


def get_temp_path(file_name):
    """
    Get path for saving temporary files.

    Args:
        file_name(string): file name
    """
    temp_path = os.path.join(os.getcwd(), 'temp')
    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    path = os.path.join(temp_path, file_name)
    return path
