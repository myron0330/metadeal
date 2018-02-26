# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Event base.
# **********************************************************************************#
from .. core.objects import ValueObject


class Event(ValueObject):

    __slots__ = [
        'event_type',
        'event_handler',
        'event_parameters'
    ]

    def __init__(self, event_type=None, event_handler=None, **kwargs):
        self.event_type = event_type
        self.event_handler = event_handler
        self.event_parameters = kwargs


class EventType(object):

    general = 'general'     # this event is system level and can not be changed or overridden.

    send_order = 'send_order'
