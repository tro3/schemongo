#!/usr/bin/env python

from copy import deepcopy
from db_doc import DBDoc, enforce_ids, merge
from diff import diff_recursive


class CollectionWrapper(object):
    def __init__(self, collection, db):
        self._collection = collection
        self._db = db
                
    def _get_max_id(self):
        return self._collection.find().count() and self._collection.find().sort('_id',-1).limit(1)[0]['_id']

    def find(self, spec=None, fields=None, skip=0, limit=0, sort=None):
        raw_cursor = self._collection.find(
            spec = spec,
            skip = skip,
            limit = limit,
            sort = sort
        )
        proj_cursor = fields and self._collection.find(
            spec = spec,
            fields = fields,
            skip = skip,
            limit = limit,
            sort = sort
        )
        return CursorWrapper(raw_cursor, proj_cursor)    

    def find_one(self, spec_or_id, fields=None, skip=0, sort=None):
        raw = self._collection.find_one(
            spec_or_id = spec_or_id,
            skip = skip,
            sort = sort            
        )
        if raw is None:
            return None
        proj = fields and self._collection.find_one(
            spec_or_id = spec_or_id,
            fields = fields,
            skip = skip,
            sort = sort
        )
        return DBDoc(raw, None, proj)
        

    def insert(self, doc_or_docs, username=None):
        if not isinstance(doc_or_docs, list):
            if isinstance(doc_or_docs, dict):
                docs = [DBDoc(doc_or_docs)]
            elif isinstance(doc_or_docs, DBDoc):
                docs = [doc_or_docs]
            else:
                raise TypeError, item
        else:
            docs = []
            for item in doc_or_docs:
                if isinstance(item, dict):
                    docs.append(DBDoc(item))
                elif isinstance(item, DBDoc):
                    docs.append(item)
                else:
                    raise TypeError, item
        
        ids = []
        new_id = self._get_max_id() + 1

        for item in docs:
            new_id = enforce_ids(item, new_id)
            id = self._collection.insert(item)
            
            if id:
                self._db.history_insert(
                    collection = self._collection.name,
                    id = id,
                    username = username
                )
            
            ids.append(id)
        
        if isinstance(doc_or_docs, list):
            return ids
        else:
            return ids[0]


    def update(self, doc, username=None, direct=False):
        assert '_id' in doc, "Cannot update document without _id attribute"
        data = DBDoc(self._collection.find_one(doc['_id']))
        old = deepcopy(data)
        if direct:
            data = doc
        else:
            merge(data, doc)
        enforce_ids(data, doc['_id'])
        result = self._collection.update({'_id': doc['_id']}, data)

        if result.get('ok', False):
            changes = diff_recursive(data, old)
            if changes:
                self._db.history_update(
                    collection = self._collection.name,
                    id = doc['_id'],
                    username = username,
                    diff = changes
                )

        return result
        

    def remove(self, spec_or_id, username=None):
        if isinstance(spec_or_id, dict):
            data = self._collection.find(spec_or_id)
        else:
            data = self._collection.find({'_id': spec_or_id})
        data = [x for x in data]
            
        result = self._collection.remove(spec_or_id)

        if result.get('ok', False):
            for item in data:
                self._db.history_remove(
                    collection = self._collection.name,
                    id = item['_id'],
                    username = username,
                    data = item
                )
            
        return result




class CursorWrapper(object):
    def __init__(self, raw_cursor, projected_cursor = None):
        self._raw_cursor = raw_cursor
        self._projected_cursor = projected_cursor
            
    def __getitem__(self, index):
        return DBDoc(self._raw_cursor[index], None, self._projected_cursor and self._projected_cursor[index])
        
    def count(self):
        return self._raw_cursor.count()












