#!/usr/bin/env python

from ..db_layer import database
from ..db_layer.db_doc import DBDoc
from schema_doc import enforce_datatypes, merge, run_auto_funcs, generate_prototype, \
                       enforce_schema_behaviors, is_object, is_list_of_objects

from pprint import pprint as p


class SchemaDatabaseWrapper(database.DatabaseWrapper):
    def __init__(self, *args, **kwords):
        super(SchemaDatabaseWrapper, self).__init__(*args, **kwords)
        self.schemas = {}
        
    def register_schema(self, key, schema):
        self._add_id_recursive(schema)
        self.schemas[key] = schema
        
    def _add_id_recursive(self, schema):
        schema.update({'_id':{'type':'integer'}})
        for key, val in schema.items():
            if is_object(val):
                self._add_id_recursive(schema[key]['schema'])
            elif is_list_of_objects(val):
                self._add_id_recursive(schema[key]['schema']['schema'])
    
    def __getattr__(self, key):
        return SchemaCollectionWrapper(self.schemas[key], self._db[key], self)
        


class SchemaCollectionWrapper(object):
    def __init__(self, schema, collection, db):
        self.schema = schema
        self.coll = database.CollectionWrapper(collection, db)

    def find(self, spec=None, fields=None, skip=0, limit=0, sort=None):
        return self.coll.find(spec, fields, skip, limit, sort) 

    def find_one(self, spec_or_id, fields=None, skip=0, sort=None):
        return self.coll.find_one(spec_or_id, fields, skip, sort)
    
    def insert(self, doc_or_docs, username=None, direct=False):
        if not isinstance(doc_or_docs, list):
            docs = [doc_or_docs]
        else:
            docs = doc_or_docs
            
        datas = []
        for incoming in docs:
            data = generate_prototype(self.schema)
            if not direct:
                errs = enforce_datatypes(self.schema, incoming)
                if errs:
                    return errs
            merge(data, incoming)
            run_auto_funcs(self.schema, data)
            if not direct:
                errs = enforce_schema_behaviors(self.schema, data, self)
                if errs:
                    return errs
            datas.append(data)
        
        self.coll.insert(datas, username)

    def update(self, incoming, username=None, direct=False):
        assert '_id' in incoming, "Cannot update document without _id attribute"

        data = self.find_one({"_id":incoming["_id"]})
        if not direct:
            errs = enforce_datatypes(self.schema, incoming)
            if errs:
                return errs
        merge(data, incoming)
        run_auto_funcs(self.schema, data)
        if not direct:
            errs = enforce_schema_behaviors(self.schema, data, self)
            if errs:
                return errs
            
        self.coll.update(data, username)

    
class CursorWrapper(object):
    def __init__(self, schema, cursor):
        self.schema = schema
        self.cursor = cursor
            
    def __getitem__(self, index):
        return DBDoc(self.cursor[index])
        
    def count(self):
        return self.cursor.count()

