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
    event_handle_data = 'handle_data'

    event_subscribe_trade = 'subscribe_trade'

    event_deal_with_trade = 'deal_with_trade'
    event_deal_with_order = 'deal_with_order'

    event_on_bar = 'on_bar'
    event_on_portfolio = 'on_portfolio'
    event_start = 'start'
    event_stop = 'stop'

    @classmethod
    def all_events(cls):
        """
        Registered events.
        """
        return cls.__events__

    @classmethod
    def strategy_events(cls):
        """
        Strategy events.
        """
        return [
            cls.event_send_order,
            cls.event_cancel_order,
            cls.event_on_tick,
            cls.event_on_order,
            cls.event_on_trade,
            cls.event_order_book,
            cls.event_on_log,
            cls.event_handle_data
        ]

    @classmethod
    def pms_events(cls):
        """
        PMS events.
        """
        return [
            cls.event_send_order,
            cls.event_cancel_order,
            cls.event_deal_with_trade,
            cls.event_deal_with_order
        ]

    @classmethod
    def subscriber_events(cls):
        """
        Subscriber events.
        """
        return [
            cls.event_subscribe_trade,
        ]

    @classmethod
    def trading_events(cls):
        """
        Trading engine events.
        """
        return [
            cls.event_on_bar,
            cls.event_on_portfolio,
            cls.event_start,
            cls.event_stop
        ]
