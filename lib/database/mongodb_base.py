from pymongo import MongoClient, UpdateOne
from .. configs import (
    mongodb_url,
    mongodb_db_name,
    mongodb_authenticate,
    mongodb_username,
    mongodb_password
)


class BatchTool(object):

    def __init__(self, collection, buffer_size=2000):
        self.buffer_size = buffer_size
        self.buffer = []
        self.collection = collection

    def append(self, query, update):
        self.buffer.append(UpdateOne(query, update, upsert=True))
        self.check_and_commit()

    def check_and_commit(self):
        if len(self.buffer) == self.buffer_size:
            self.commit()

    def commit(self):
        if self.buffer:
            self.collection.bulk_write(self.buffer, ordered=False)
            del self.buffer[:]

    def end(self):
        self.commit()


mongodb_client = MongoClient(host=mongodb_url, connect=False)
mongodb_database = mongodb_client[mongodb_db_name]
if mongodb_authenticate:
    mongodb_database.authenticate(mongodb_username, mongodb_password)


__all__ = [
    'mongodb_client',
    'mongodb_database',
    'BatchTool'
]
