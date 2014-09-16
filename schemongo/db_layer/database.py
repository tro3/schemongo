
import datetime
from pymongo import MongoClient
from collection import CollectionWrapper


class DatabaseWrapper(object):
    def __init__(self, client=None, dbname=None):
        self._client = client or MongoClient()
        self._db = self._client[dbname or 'test']
        self.history = self._db._history
        
    def __getattr__(self, key):
        return CollectionWrapper(self._db[key], self)


    def history_find(self, spec=None, fields=None, skip=0, limit=0, sort=None):
        return self._db._history.find(
            spec = spec,
            skip = skip,
            limit = limit,
            sort = sort
        )
    
    
    def history_insert(self, collection, id, username):
        self._db._history.insert(
            {
                'collection': collection,
                'id':  id,
                'time': datetime.datetime.now(),
                'username': username,
                'action': 'document created'
            }            
        )

    def history_update(self, collection, id, username, diff):
        self._db._history.insert(
            {
                'collection': collection,
                'id':  id,
                'time': datetime.datetime.now(),
                'username': username,
                'changes': diff
            }            
        )
    
    def history_remove(self, collection, id, username, data):
        self._db._history.insert(
            {
                'collection': collection,
                'id':  id,
                'time': datetime.datetime.now(),
                'username': username,
                'action': 'document removed',
                'data': data                
            }            
        )
