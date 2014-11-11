#!/usr/bin/env python

import datetime
import dateutil.parser
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
    if isinstance(val, datetime.datetime):
        return val
    if type(val) in [str, unicode]:
        return dateutil.parser.parse(val)
    raise

def _convert_reference(val):
    return val['_id']

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
        raise TypeError, "Improper schema type '%s'" % _type
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


def enforce_schema_behaviors(schema, data, db_coll, path=''):
    errs = []
    for key in [x for x in data.keys() if x in schema]:
        path = path and (path + '/')
        if is_object(schema[key]):
            errs.extend(enforce_schema_behaviors(schema[key]['schema'], data[key], db_coll, path + key))
        elif is_list_of_objects(schema[key]):
            for i, item in enumerate(data[key]):
                errs.extend(enforce_schema_behaviors(schema[key]['schema']['schema'], item, db_coll, path + '%s/%s' % (key, i)))
        elif schema[key]['type'] == 'list' and 'schema' in schema[key] and 'allowed' in schema[key]['schema']:
            for i, item in enumerate(data[key]):
                if not check_allowed(schema[key]['schema']['allowed'], data[key][i], data[key]):
                    errs.append('%s%s/%s: %s' % (path, key, i, "'%s' not one of the allowed values" % data[key][i]))                
        else:
            if 'allowed' in schema[key]:
                if not check_allowed(schema[key]['allowed'], data, data[key]):
                    errs.append('%s%s: %s' % (path, key, "'%s' not one of the allowed values" % data[key]))
            if 'required' in schema[key] and schema[key]['required']:
                if key not in data or data[key] is None:
                    errs.append('%s%s: %s' % (path, key, "value is required"))
            if 'unique' in schema[key] and schema[key]['unique']:
                if db_coll.find({key: data[key], '_id': {'$ne': data.get('_id', 0)}}).count():
                    errs.append('%s%s: %s' % (path, key, "'%s' is not unique" % data[key]))
                        
    return errs


def check_allowed(allowed, elem, data):
    if callable(allowed):
        allowed = allowed(elem)
    if not hasattr(allowed, '__iter__'):
        raise "'required' parameter '%s' did not evaluate to an iterable" % allowed
    if data not in allowed:
        return False
    return True




def generate_prototype(schema, parent=None):
    result = {}
    for key in schema.keys():
        if 'serialize' in schema[key]:
            continue
        result[key] = _generate_prototype_field(schema[key])
    return DBDoc(result, parent)


def _generate_prototype_field(schema, parent=None):
    if is_object(schema):
        return generate_prototype(schema['schema'], parent)
    elif is_list_of_objects(schema):
        return DBDocList([], parent)
    elif 'default' in schema:
        return schema['default']
    elif schema['type'] == 'dict':
        return {}
    elif schema['type'] == 'list':
        return []
    else:
        return None


def fill_in_prototypes(schema, doc):
    for key in schema.keys():
        if key == '_id':
            continue
        if key not in doc and 'serialize' not in schema[key]:
            doc[key] = _generate_prototype_field(schema[key], doc)

        if is_object(schema[key]):
            doc[key] = doc[key] or {}
            fill_in_prototypes(schema[key]['schema'], doc[key])
        elif is_list_of_objects(schema[key]):
            doc[key] = doc[key] or []
            [fill_in_prototypes(schema[key]['schema']['schema'], item) for item in doc[key]]
            
    for key in doc.keys():
        if key not in schema:
            doc.pop(key)
        

def run_auto_funcs(schema, data):
    for key in schema.keys():
        if is_object(schema[key]):
            run_auto_funcs(schema[key]['schema'], data[key])
        elif is_list_of_objects(schema[key]):
            [run_auto_funcs(schema[key]['schema']['schema'], x) for x in data[key]]
        elif 'auto_init' in schema[key] and not data._id:
            data[key] = schema[key]['auto_init'](data)
            data[key] = _convert_value(schema[key], data[key])
        elif 'auto' in schema[key]:
            data[key] = schema[key]['auto'](data)
            data[key] = _convert_value(schema[key], data[key])


