
import datetime
from dateutil.tz import tzlocal
from pymongo import MongoClient
from collection import CollectionWrapper


class DatabaseWrapper(object):
    def __init__(self, client=None, dbname=None):
        self._client = client or MongoClient(tz_aware=True)
        self._db = self._client[dbname or 'test']
        self.history = self._db._history
        
    def __getattr__(self, key):
        return CollectionWrapper(self._db[key], self)
    
    
    def get_next_id(self, collection):
        if not self._db._ids.find({'collection': collection}).count():
            return 1
        return self._db._ids.find_one({'collection': collection})['last_id'] + 1


    def set_last_id(self, collection, id):
        if not self._db._ids.find({'collection': collection}).count():
            self._db._ids.insert({'collection': collection, 'last_id':id})
        else:
            self._db._ids.update({'collection': collection}, {'collection': collection, 'last_id':id})
    


    def history_find(self, spec=None, fields=None, skip=0, limit=0, sort=None):
        sort = sort or [('_id',1)]
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
                'time': datetime.datetime.now().replace(tzinfo=tzlocal()),
                'username': username,
                'action': 'document created'
            }            
        )

    def history_update(self, collection, id, username, diff):
        self._db._history.insert(
            {
                'collection': collection,
                'id':  id,
                'time': datetime.datetime.now().replace(tzinfo=tzlocal()),
                'username': username,
                'changes': diff
            }            
        )
    
    def history_remove(self, collection, id, username, data):
        self._db._history.insert(
            {
                'collection': collection,
                'id':  id,
                'time': datetime.datetime.now().replace(tzinfo=tzlocal()),
                'username': username,
                'action': 'document removed',
                'data': data                
            }            
        )
