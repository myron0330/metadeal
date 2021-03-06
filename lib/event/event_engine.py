# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Event engine
# **********************************************************************************#
from Queue import Queue, Empty
from threading import Thread
from utils.error import HandleDataException
from utils.dict import DefaultDict
from . event_base import Event, EventType


class EventEngine(object):
    """
    Event engine: controller system of event.
    """
    def __init__(self,
                 active=False,
                 event_queue=None,
                 event_handlers=None,
                 log=None):
        self._active = active
        self._event_queue = event_queue or Queue()
        self._event_handlers = event_handlers or DefaultDict(list)
        self._processor = Thread(target=self._run)
        self._log = log

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

    def is_active(self):
        """
        Whether the process is alive.
        """
        return self._active

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
            except HandleDataException:
                self._active = False
            except:
                import traceback
                self._log.error(u'执行策略失败 %s' % str(traceback.format_exc()))
                self._active = False

    def _process(self, event):
        """
        Processor of event.
        Args:
            event(Event): event instance.
        """
        if event.event_type in self._event_handlers:
            map(lambda handler: handler(**event.event_parameters), self._event_handlers[event.event_type])

        map(lambda handler: handler(**event.event_parameters), self._event_handlers[EventType.general])
