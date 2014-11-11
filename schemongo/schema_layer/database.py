#!/usr/bin/env python

from ..db_layer import database
from ..db_layer.collection import CursorWrapper
from ..db_layer.db_doc import DBDoc
from schema_doc import enforce_datatypes, merge, run_auto_funcs, generate_prototype, fill_in_prototypes, \
                       enforce_schema_behaviors, is_object, is_list_of_objects, is_list_of_references
from serialization import serialize, serialize_list, get_serial_dict, get_serial_list

from pprint import pprint as p


class SchemaDatabaseWrapper(database.DatabaseWrapper):
    def __init__(self, *args, **kwords):
        super(SchemaDatabaseWrapper, self).__init__(*args, **kwords)
        self.schemas = {}
        self.references = {}
        
    def register_schema(self, key, schema):
        self._prep_schema(schema, key)
        self.schemas[key] = schema
        
    def _prep_schema(self, schema, coll_name):
        schema.update({'_id':{'type':'integer'}})
        for key, val in schema.items():
            if is_object(val):
                self._prep_schema(schema[key]['schema'], coll_name)
            elif is_list_of_objects(val):
                self._prep_schema(schema[key]['schema']['schema'], coll_name)
            elif is_list_of_references(val):
                remote = val['schema']['collection']
                self.references[remote] = self.references.get(remote, [])
                self.references[remote].append(coll_name)
            elif val['type'] == 'reference':
                remote = val['collection']
                self.references[remote] = self.references.get(remote, [])
                self.references[remote].append(coll_name)
                
    def check_singular_references(self, coll_name, id):
        for coll in self.references.get(coll_name,[]):
            schema = self.schemas[coll]
            err_list = []
            for item in self[coll].find():
                errs = self._check_singular_references_recursive(item, schema, coll_name, id)
                if errs:
                    err_list.append("Collection '%s', item %s: undeleteable reference encountered" % (coll, item['_id']))
            if err_list:
                return err_list
    
    def _check_singular_references_recursive(self, item, schema, coll_name, id):
        for key, val in schema.items():
            if is_object(val):
                errs = self._check_singular_references_recursive(item[key], schema[key]['schema'], coll_name, id)
                if errs:
                    return errs                
            elif is_list_of_objects(val):
                for x in item[key]:
                    errs = self._check_singular_references_recursive(x, schema[key]['schema']['schema'], coll_name, id)             
                    if errs:
                        return errs
            elif val['type'] == 'reference' and 'required' in val and val['required']:
                if item[key]['_id'] == id:
                    return True

    def remove_references(self, coll_name, id):
        for coll in self.references.get(coll_name,[]):
            schema = self.schemas[coll]
            for item in self[coll].find():
                if self._remove_references_recursive(item, schema, coll_name, id):
                    self[coll].update(item)

    def _remove_references_recursive(self, item, schema, coll_name, id):
        result = False
        for key, val in schema.items():
            if is_object(val):
                result = result or self._remove_references_recursive(item[key], schema[key]['schema'], coll_name, id)
            elif is_list_of_objects(val):
                for x in item[key]:
                    result = result or self._remove_references_recursive(x, schema[key]['schema']['schema'], coll_name, id)             
            elif is_list_of_references(val):
                if id in [x['_id'] for x in item[key]]:
                    item[key] = [x for x in item[key] if x['_id'] != id]
                    result = True
            elif val['type'] == 'reference' and item[key]['_id'] == id:
                item[key] = None
                result = True
        return result
                
    
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
        if not tmp:
            return
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

        data = self.coll.find_one({"_id":incoming["_id"]})
        merge(data, incoming)
        fill_in_prototypes(self.schema, data)
        run_auto_funcs(self.schema, data)

        errs = enforce_schema_behaviors(self.schema, data, self)
        if errs:
            return (None, errs)
            
        return (data, [])


    def process_direct_update(self, incoming):
        data = self.coll.find_one({"_id":incoming["_id"]})
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
        if isinstance(spec_or_id, dict):
            data = self.coll.find(spec_or_id)
        else:
            data = self.coll.find({'_id': spec_or_id})
        data = [x for x in data]
        
        for item in data:
            errs = self.db.check_singular_references(self.coll._collection.name, item._id)
            if errs:
                return errs

        self.db.remove_references(self.coll._collection.name, item._id)
        
        self.coll.remove(spec_or_id, username)


    def serialize(self, item):
        return serialize(self.schema, item)


    def get_serial_dict(self, item):
        return get_serial_dict(self.schema, item)


    def serialize_list(self, items):
        return serialize_list(self.schema, items)


    def find_and_serialize(self, spec=None, fields=None, skip=0, limit=0, sort=None):
        return self.serialize_list(self.find(spec, fields, skip, limit, sort))


    def find_one_and_serialize(self, spec_or_id, fields=None, skip=0, sort=None):
        return self.serialize(self.find_one(spec_or_id, fields, skip, sort))


    def find_and_serial_dict(self, spec=None, fields=None, skip=0, limit=0, sort=None):
        return get_serial_list(self.schema, self.find(spec, fields, skip, limit, sort))


    def find_one_and_serial_dict(self, spec_or_id, fields=None, skip=0, sort=None):
        return get_serial_dict(self.schema, self.find_one(spec_or_id, fields, skip, sort))



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
            if key in data:
                expand_references(db, schema[key]['schema'], data[key])
            else:
                data[key] = None
        elif is_list_of_objects(schema[key]):
            if key in data:
                [expand_references(db, schema[key]['schema']['schema'], x) for x in data[key]]
            else:
                data[key] = []
        elif is_list_of_references(schema[key]):
            if key in data:
                data[key] = [_expand_single_reference(db, schema[key]['schema'], x) for x in data[key] if data[key]]
            else:
                data[key] = []
        elif schema[key]['type'] == 'reference':
            if key in data:
                data[key] = data[key] and _expand_single_reference(db, schema[key], data[key])
            else:
                data[key] = None
                
                
def _expand_single_reference(db, schema, _id):
    result = db[schema['collection']].find_one({'_id':_id}, fields=schema.get('fields', None))
    if result:
        result.__schema = db.schemas[schema['collection']]
        return result
    else:
        return 'reference not found'
