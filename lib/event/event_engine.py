# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Event engine
# **********************************************************************************#
from queue import Queue, Empty
from threading import Thread
from . event_base import Event, EventType
from .. utils.dict_utils import DefaultDict


class EventEngine(object):
    """
    Event engine: controller system of event.
    """
    def __init__(self,
                 active=False,
                 event_queue=None,
                 event_handlers=None):
        self._active = active
        self._event_queue = event_queue or Queue()
        self._event_handlers = event_handlers or DefaultDict(list)
        self._processor = Thread(target=self._run)

    def start(self):
        """
        Start the event engine.
        """
        self._active = True
        self._processor.start()

    def stop(self):
        """
        Stop the event engine
        """
        self._active = False
        self._processor.join()

    def publish(self, event_type, **kwargs):
        """
        Publish event
        Args:
            event_type(string): event type
            **kwargs: key-value parameters
        """
        event = Event(event_type=event_type, **kwargs)
        self.put_event(event)

    def put_event(self, event):
        """
        Put event to processing queue.
        Args:
            event(Event): event instance.
        """
        self._event_queue.put(event)

    def register_handlers(self, event_type, handler):
        """
        Register handlers.
        Args:
            event_type(string): event type.
            handler(func): callable function.
        """
        if isinstance(handler, (list, set, tuple)):
            self._event_handlers[event_type] += list(handler)
        else:
            self._event_handlers[event_type].append(handler)

    def remove_handlers(self, event_type, handler):
        """
        Remove handlers.
        Args:
            event_type(string): event type.
            handler(func): callable function.
        """
        if isinstance(handler, (list, set, tuple)):
            for _ in handler:
                self._event_handlers[event_type].remove(_)
        else:
            self._event_handlers[event_type].remove(handler)

    def _run(self, timeout=0.5):
        """
        Run worker loop.
        """
        while self._active:
            try:
                event = self._event_queue.get(block=True, timeout=timeout)
                self._process(event)
            except Empty:
                pass

    def _process(self, event):
        """
        Processor of event.
        Args:
            event(Event): event instance.
        """
        if event.event_type in self._event_handlers:
            map(lambda handler: handler(**event.event_parameters), self._event_handlers[event.event_type])

        map(lambda handler: handler(**event.event_parameters), self._event_handlers[EventType.general])
