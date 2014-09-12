#!/usr/bin/env python

from ..db_layer.db_doc import DBLayerDoc, enforce_ids

"""
data types                      on_write                            on_serialize
    boolean
    integer
    float
    string
    datetime                    convert from iso                    write as iso
    reference                   collapse to ref_field               expand to include fields
    internal_reference          collapse to ref_field               expand to include fields
    
    list of datatype
    hash of basic datatype : basic datatype (no dates or references)


behaviors
    default                     add if not present (create only)
    auto                        overwrite incoming
    auto_init                   overwrite incoming (create only)
    serialize                   remove                              add
    read_only                   remove
    

"""

def is_object(val):
    assert isinstance(val ,dict), val
    return val['type'] == 'dict'    \
       and 'schema' in val                 

def is_list_of_objects(val):
    assert isinstance(val ,dict), val
    return val['type'] == 'list'    \
       and 'schema' in val          \
       and is_object(val['schema'])

def is_list_of_references(val):
    assert isinstance(val ,dict), val
    return val['type'] == 'list'    \
       and 'schema' in val          \
       and val['schema']['type'] == 'reference'

def is_read_only(val):
    read_only_attributes = ['auto', 'auto_init', 'serialize', 'read_only']
    return any(x in val for x in read_only_attributes)



class SchemaDoc(dict):
    def __init__(self, schema, raw, parent=None, projection=None):
        dict.__init__(self)
        self.parent = parent

        self._schema = schema
        self._schema.update({'_id': {'type':'integer'}})

        if not projection and isinstance(raw, DBLayerDoc):
            projection = raw._projection
        self._projection = projection
        
        for key, val in self._schema.items():
            if key in raw:
                if is_object(val):
                    self[key] = SchemaDoc(val['schema'], raw[key], self, None)
                elif is_list_of_objects(val):
                    self[key] = SchemaList(val['schema']['schema'], raw[key], self)
                else:
                    self[key] = raw[key]

    def __getattr__(self, key):
        if key not in self:
            raise AttributeError, key
        return self[key]
    
    def root(self):
        current = self
        while current.parent:
            current = current.parent
        return current
    
            

            
class SchemaList(list):
    def __init__(self, schema, raw, parent=None):
        list.__init__(self)
        self._schema = schema
        self.parent = parent

        for i, val in enumerate(raw):
            self.append(SchemaDoc(self._schema, val, self))



# Collapsing

def _convert_datetime(val):
    if isinstance(value, datetime.datetime):
        return value
    if type(value) in [str, unicode]:
        return dateutil.parser.parse(value)
    raise

def _convert_reference(val):
    pass

def _convert_internal_ref(val):
    pass

_converters = {
    'boolean': bool,
    'integer': int,
    'float': float,
    'string': str,
    'dict': dict,
    'datetime': _convert_datetime,
    'reference': _convert_reference,
    'internal_ref': _convert_internal_ref,
}


def _convert_value(schema, value):
    _type = schema['type']
    if value is None:
        return None
    try:
        if _type in _converters:
            return _converters[_type](value)
        if _type == 'list':
            if 'schema' in schema:
                subtype = schema['schema']['type']
                _type = 'list of %ss' % subtype
                tmp = [_converters[subtype](x) for x in value]
                _type = 'list'
                return tmp
            else:
                return list(value)
    except:
        raise TypeError, "Could not convert '%s' to type '%s'" % (value, _type)


def enforce_schema(schema, data):
    """
    Note schema metadata (e.g. type) owned my parent
    data is primitive dict, intended at incoming data
    """
    for key in data.keys():
        if key not in schema or is_read_only(schema[key]):
            data.pop(key)
        elif is_object(schema[key]):
            enforce_schema(schema[key]['schema'], data[key])
        elif is_list_of_objects(schema[key]):
            map(lambda x: enforce_schema(schema[key]['schema']['schema'], x), data[key])
        else:
            data[key] = _convert_value(schema[key], data[key])

        
def generate_prototype(schema):
    result = {}
    for key in schema.keys():
        if is_object(schema[key]):
            result[key] = generate_prototype(schema[key]['schema'])
        elif is_list_of_objects(schema[key]):
            result[key] = SchemaList(schema[key]['schema']['schema'], [], result)
        elif 'default' in schema[key]:
            result[key] = schema[key]['default']
        elif schema[key]['type'] == 'dict':
            result[key] = {}
        elif schema[key]['type'] == 'list':
            result[key] = []
        else:
            result[key] = None
    return SchemaDoc(schema, result)
    

def run_auto_funcs(data):
    "'data' must be SchemaDoc"
    if not isinstance(data, SchemaDoc): import pdb; pdb.set_trace()
    schema = data._schema
    for key in schema.keys():
        if is_object(schema[key]):
            run_auto_funcs(data[key])
        elif is_list_of_objects(schema[key]):
            map(run_auto_funcs, data[key])
        elif 'auto_init' in schema[key] and '_id' not in data:
            data[key] = schema[key]['auto_init'](data)
        elif 'auto' in schema[key]:
            data[key] = schema[key]['auto'](data)
            
    
def merge(schema, original, new):
    "Assumes new has already been had schema enforced"
    for key, val in new.items():
        if key in original:
            if is_object(schema[key]):
                merge(schema[key]['schema'], original[key], val)
            elif is_list_of_objects(schema[key]) and len(val):
                old = original[key]
                ids = [x['_id'] for x in old]
                original[key] = SchemaList(schema[key], [], original[key].parent)
                for doc in val:
                    if '_id' in doc and doc['_id'] in ids:
                        new = old[ids.index(doc['_id'])]
                        merge(schema[key]['schema']['schema'], new, doc)
                        original[key].append(new)
                    else:
                        original[key].append(SchemaDoc(schema[key]['schema']['schema'], doc, original[key]))
            else:
                original[key] = val
        else:
            if is_object(schema[key]):
                original[key] = SchemaDoc(schema[key]['schema'], val, original)
            elif is_list_of_objects(schema[key]) and len(val):
                original[key] = SchemaList(schema[key]['schema']['schema'], val, original)
            else:
                original[key] = val
