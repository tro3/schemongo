#!/usr/bin/env python

from ..db_layer import database
from schema_doc import SchemaDoc, enforce_schema, merge, run_auto_funcs, generate_prototype


class SchemaDatabaseWrapper(database.DatabaseWrapper):
    def __init__(self, *args, **kwords):
        super(SchemaDatabaseWrapper, self).__init__(*args, **kwords)
        self.schemas = {}
    
    def __getattr__(self, key):
        return SchemaCollectionWrapper(self.schemas[key], self._db[key], self)
        

class SchemaCollectionWrapper(object):
    def __init__(self, schema, collection, db):
        self.schema = schema
        self.coll = database.CollectionWrapper(collection, db)

    def find(self, spec=None, fields=None, skip=0, limit=0, sort=None):
        return SchemaCursorWrapper(self.coll(spec, fields, skip, limit, sort))  

    def find_one(self, spec_or_id, fields=None, skip=0, sort=None):
        return SchemaDoc(self.schema, self.coll.find_one(spec_or_id, fields, skip, sort))
    
    def insert(self, doc_or_docs, username=None):
        if not isinstance(doc_or_docs, list):
            docs = [doc_or_docs]
        else:
            docs = doc_or_docs
            
        datas = []
        for incoming in docs:
            data = generate_prototype(self.schema)
            enforce_schema(self.schema, incoming)
            merge(self.schema, data, incoming)
            run_auto_funcs(data)
            datas.append(data)
            
        self.coll.insert(datas, username)

    def update(self, incoming, username=None):
        assert '_id' in incoming, "Cannot update document without _id attribute"

        data = self.find_one({"_id":incoming["_id"]})
        enforce_schema(self.schema, incoming)
        merge(self.schema, data, incoming)
        run_auto_funcs(data)
            
        self.coll.update(data, username)

    
class CursorWrapper(object):
    def __init__(self, schema, cursor):
        self.schema = schema
        self.cursor = cursor
            
    def __getitem__(self, index):
        return SchemaDoc(self.schema, self.cursor[index])
        
    def count(self):
        return self.cursor.count()

