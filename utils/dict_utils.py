# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Custom dict structures for doing interesting things.
# **********************************************************************************#
from copy import deepcopy


class AttributeDict(dict):

    """
    A dict that allows direct attribute access to its keys.
    """

    def __getattr__(self, item):
        if item in self.__dict__:
            return self.__dict__.__getitem__(item)
        elif item in self:
            return self.__getitem__(item)
        else:
            raise AttributeError("'dict' object has no attribute '{}'".format(item))

    def __setattr__(self, key, value):
        if key in self.__dict__:
            self.__dict__.__setitem__(key, value)
        elif key in self:
            self.__setitem__(key, value)


class CompositeDict(dict):

    """
    A dict that allows to be used as a composite one without tedious initialization.
    """
    def __missing__(self, key):
        self.__setitem__(key, CompositeDict())
        return self.__getitem__(key)


class DefaultDict(dict):

    """
    A dict that allows set default value for any key(exist or in-exist).
    """
    def __init__(self, default=None, **kwargs):
        """
        Args:
            default(class or object): the specified default type or instance.
        """
        super(DefaultDict, self).__init__()
        self._default = default
        self._kwargs = kwargs

    def __missing__(self, key):
        default = deepcopy(self._default(**self._kwargs)) if isinstance(self._default, type) else deepcopy(self._default)
        self.__setitem__(key, default)
        return self.__getitem__(key)
