# -*- coding: UTF-8 -*-
# **********************************************************************************#
#     File: Redis base file
# **********************************************************************************#
import json
import redis
from .. configs import redis_host, redis_port


class RedisCollection(object):

    order = 'order'
    position = 'position'
    trade = 'trade'


class RedisSet(object):

    def __init__(self, client, key='changed_portfolio_set'):
        self.client = client
        self.key = key

    def add_elements_to_set(self, elements, key=None):
        """
        Add elements to set.
        Args:
            elements(list): element list
            key(string): redis key

        Returns:

        """
        key = key or self.key
        self.client.sadd(key, *elements)

    def get_and_pop_all_elements(self, key=None):
        """
        Get and pop all elements
        Args:
            key(string): redis key

        Returns:
            list: result
        """
        key = key or self.key
        result = list()
        while self.client.scard(key) > 0:
            item = self.client.spop(key)
            if item:
                result.append(item)
        return result


class RedisQueue(object):

    def __init__(self, client, key='queue'):
        """
        Redis queue

        Args:
            client: redis client
            key(string): redis key
        """
        self.client = client
        self.key = key

    def size(self, key=None):
        """
        Return current size of the queue

        Args:
            key(string): specific redis queue key
        """
        key = key or self.key
        return self.client.llen(key)

    def empty(self, key=None):
        """
        Return True if the queue is empty, False otherwise

        Args:
            key(string): specific redis queue key
        """
        key = key or self.key
        return self.size(key) == 0

    def put(self, items, key=None):
        """
        Put item or item list into the queue

        Args:
            items(string or list): items
            key(string): specific redis queue key
        """
        key = key or self.key
        if isinstance(items, (str, unicode)):
            self.client.rpush(key, items)
        elif isinstance(items, (tuple, list)):
            items = map(json.dumps, items)
            self.client.rpush(key, *items)

    def get(self, key=None, block=True, timeout=None):
        """
        Remove and return an item from the queue.
        Args:
            key(string): specific redis queue key
            block(boolean): if block is true and timeout is None (the default),
                            block if necessary until an item is available
            timeout(float): time out setting.
        """
        key = key or self.key
        if block:
            item = self.client.blpop(key, timeout=timeout)
        else:
            item = (None, self.client.lpop(key))
        if item:
            item = item[1]
        return json.loads(item)

    def get_nowait(self, key=None):
        """
        Equivalent to get(False)

        Args:
            key(string): specific redis queue key
        """
        key = key or self.key
        return self.get(key, False)

    def get_all(self, key=None):
        """
        Get all items from key

        Args:
            key(string): specific redis queue key
        """
        key = key or self.key
        items = list()
        while self.size(key):
            items.append(self.get_nowait(key))
        return items

    def clear(self, key=None):
        """
        Clear redis queue

        Args:
            key(string): specific redis queue key
        """
        key = key or self.key
        self.client.ltrim(key, self.size(key), self.size(key))


_pool = redis.ConnectionPool(host=redis_host, port=redis_port)
redis_client = redis.Redis(connection_pool=_pool)
redis_queue = RedisQueue(redis_client)
redis_set = RedisSet(redis_client)
