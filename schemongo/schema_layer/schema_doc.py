#!/usr/bin/env python

from ..db_layer.db_doc import DBDoc, DBDocList, enforce_ids, merge

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


def enforce_datatypes(schema, data, path=''):
    """
    Note schema metadata (e.g. type) owned my parent
    data is primitive dict, intended at incoming data
    """
    errs = []
    for key in data.keys():
        path = path and (path + '/')
        if key not in schema or is_read_only(schema[key]):
            data.pop(key)
        elif is_object(schema[key]):
            errs.extend(enforce_datatypes(schema[key]['schema'], data[key], path + key))
        elif is_list_of_objects(schema[key]):
            for i, item in enumerate(data[key]):
                errs.extend(enforce_datatypes(schema[key]['schema']['schema'], item, path + '%s/%s' % (key, i)))
        else:
            try:
                data[key] = _convert_value(schema[key], data[key])
            except Exception, e:
                errs.append('%s%s: %s' % (path, key, e.message))
    return errs

        
def generate_prototype(schema):
    result = {}
    for key in schema.keys():
        if is_object(schema[key]):
            result[key] = generate_prototype(schema[key]['schema'])
        elif is_list_of_objects(schema[key]):
            result[key] = DBDocList([], result)
        elif 'default' in schema[key]:
            result[key] = schema[key]['default']
        elif schema[key]['type'] == 'dict':
            result[key] = {}
        elif schema[key]['type'] == 'list':
            result[key] = []
        else:
            result[key] = None
    return DBDoc(result)
    

def run_auto_funcs(schema, data):
    for key in schema.keys():
        if is_object(schema[key]):
            run_auto_funcs(schema[key]['schema'], data[key])
        elif is_list_of_objects(schema[key]):
            [run_auto_funcs(schema[key]['schema']['schema'], x) for x in data[key]]
        elif 'auto_init' in schema[key] and '_id' not in data:
            if not isinstance(data, DBDoc):
                import pdb
                pdb.set_trace()
            data[key] = schema[key]['auto_init'](data)
        elif 'auto' in schema[key]:
            data[key] = schema[key]['auto'](data)
