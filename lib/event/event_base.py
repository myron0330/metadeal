# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Event base.
# **********************************************************************************#
from .. core.objects import ValueObject


class Event(ValueObject):

    __slots__ = [
        'event_type',
        'event_parameters'
    ]

    def __init__(self, event_type=None, **kwargs):
        self.event_type = event_type
        self.event_parameters = kwargs


class EventTypeMcs(type):
    """
    EventType meta class
    """
    def __new__(mcs, name, bases, attributes):
        attributes['__events__'] = [event for _, event in attributes.iteritems() if _.startswith('event_')]
        return type.__new__(mcs, name, bases, attributes)


class EventType(object):

    __metaclass__ = EventTypeMcs

    general = 'general'     # this event is system level and can not be changed or overridden.

    event_send_order = 'send_order'
    event_cancel_order = 'cancel_order'
    event_on_tick = 'on_tick'
    event_on_order = 'on_order'
    event_on_trade = 'on_trade'
    event_order_book = 'on_order_book'
    event_on_log = 'on_log'

    @classmethod
    def registered_events(cls):
        """
        Registered events.
        """
        return cls.__events__
