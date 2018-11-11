"""
# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Basic objects.
# **********************************************************************************#
"""


class SlottedObject(object):
    """
    Object with slots to shrink the memory.
    """
    __slots__ = []

    @classmethod
    def __name__(cls):
        return cls.__name__

    def __getitem__(self, key):
        return getattr(self, key)

    def __init__(self, **kwargs):
        self.__dict__.update(**kwargs)

    def get(self, key, default=None):
        """
        Get the value of key with a default value.

        Args:
            key(string): key
            default(object): default value, can be any instance

        Returns:
            instance: default class object instance
        """
        return getattr(self, key, default)

    @classmethod
    def from_dict(cls, kwargs):
        """
        Query from dict
        """
        return cls(**kwargs)

    def to_dict(self):
        """
        To dict
        """
        return {attribute: getattr(self, attribute) for attribute in self.__slots__}

    def update_(self, items):
        """
        Update info

        Args:
            items(dict): items
        """
        for key, value in items.items():
            setattr(self, key, value)

    def __repr__(self):
        """
        Representation.
        """
        return '{}({})'.format(self.__name__(), ', '.join(['{}={}'.format(key, getattr(self, key))
                                                           for key in self.__slots__]))
