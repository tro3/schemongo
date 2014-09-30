#!/usr/bin/env python

from ..db_layer import database
from ..db_layer.collection import CursorWrapper
from ..db_layer.db_doc import DBDoc
from schema_doc import enforce_datatypes, merge, run_auto_funcs, generate_prototype, fill_in_prototypes, \
                       enforce_schema_behaviors, is_object, is_list_of_objects
from serialization import serialize, serialize_list

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
        return self[key]

    def __getitem__(self, key):
        return SchemaCollectionWrapper(self.schemas[key], self._db[key], self)
        


class SchemaCollectionWrapper(object):
    def __init__(self, schema, collection, db):
        self.schema = schema
        self.db = db
        self.coll = database.CollectionWrapper(collection, db)

    def find(self, spec=None, fields=None, skip=0, limit=0, sort=None):
        return SchemaCursorWrapper(self.coll.find(spec, fields, skip, limit, sort), self.db, self.schema)

    def find_one(self, spec_or_id, fields=None, skip=0, sort=None):
        tmp = self.coll.find_one(spec_or_id, fields, skip, sort)
        expand_references(self.db, self.schema, tmp)
        return tmp
    
    
    def process_insert(self, incoming):
        errs = enforce_datatypes(self.schema, incoming)
        if errs:
            return (None, errs)
            
        data = generate_prototype(self.schema)
        merge(data, incoming)
        fill_in_prototypes(self.schema, data)
        run_auto_funcs(self.schema, data)
        
        errs = enforce_schema_behaviors(self.schema, data, self)
        if errs:
            return (None, errs)
            
        return (data, [])


    def process_direct_insert(self, incoming):
        data = generate_prototype(self.schema)
        merge(data, incoming)
        fill_in_prototypes(self.schema, data)
        run_auto_funcs(self.schema, data)
        return (data, [])
    
    
    def process_update(self, incoming):
        assert '_id' in incoming, "Cannot update document without _id attribute"

        errs = enforce_datatypes(self.schema, incoming)
        if errs:
            return (None, errs)

        data = self.find_one({"_id":incoming["_id"]})
        merge(data, incoming)
        fill_in_prototypes(self.schema, data)
        run_auto_funcs(self.schema, data)

        errs = enforce_schema_behaviors(self.schema, data, self)
        if errs:
            return (None, errs)
            
        return (data, [])


    def process_direct_update(self, incoming):
        data = self.find_one({"_id":incoming["_id"]})
        merge(data, incoming)
        fill_in_prototypes(self.schema, data)
        run_auto_funcs(self.schema, data)
        return (data, [])
    
    
    def insert(self, doc_or_docs, username=None, direct=False):
        if not isinstance(doc_or_docs, list):
            docs = [doc_or_docs]
        else:
            docs = doc_or_docs
            
        datas = []
        errs = []
        for incoming in docs:
            if direct:
                data, local_errs = self.process_direct_insert(incoming)
            else:
                data, local_errs = self.process_insert(incoming)
            datas.append(data)
            errs.append(local_errs)

        if any(errs) and len(errs) == 1:
            return ([], errs[0])
        if any(errs) and len(errs) > 1:
            return ([], errs)
        
        ids = self.coll.insert(datas, username)
        return (ids, None)


    def update(self, incoming, username=None, direct=False):
        if direct:
            data, errs = self.process_direct_update(incoming)
        else:
            data, errs = self.process_update(incoming)
        if errs:
            return errs
            
        self.coll.update(data, username, direct)


    def remove(self, spec_or_id, username=None):
        self.coll.remove(spec_or_id, username)


    def serialize(self, item):
        return serialize(self.schema, item)


    def serialize_list(self, items):
        return serialize_list(self.schema, items)


    def find_and_serialize(self, spec=None, fields=None, skip=0, limit=0, sort=None):
        return self.serialize_list(self.find(spec, fields, skip, limit, sort))


    def find_one_and_serialize(self, spec_or_id, fields=None, skip=0, sort=None):
        return self.serialize(self.find_one(spec_or_id, fields, skip, sort))



class SchemaCursorWrapper(CursorWrapper):
    def __init__(self, cursor, db, schema):
        CursorWrapper.__init__(self, cursor._raw_cursor, cursor._projected_cursor)
        self.db = db
        self.schema = schema

    def __getitem__(self, index):
        tmp = CursorWrapper.__getitem__(self, index)
        expand_references(self.db, self.schema, tmp)
        return tmp



def expand_references(db, schema, data):
    for key in schema.keys():
        if is_object(schema[key]):
            expand_references(db, schema[key]['schema'], data[key])
        elif is_list_of_objects(schema[key]):
            [expand_references(db, schema[key]['schema']['schema'], x) for x in data[key]]
        elif schema[key]['type'] == 'reference':
            data[key] = db[schema[key]['collection']].find_one({'_id':data[key]}, fields=schema[key].get('fields', None))
            if data[key]:
                data[key].__schema = db.schemas[schema[key]['collection']]