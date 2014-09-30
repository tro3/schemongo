#!/usr/bin/env python

import json
import copy
from schema_doc import is_object, is_list_of_objects


def serialize_list(schema, items):
    return json.dumps(get_serial_list(schema, items))

def serialize(schema, item):
    return json.dumps(get_serial_dict(schema, item))
    
def get_serial_dict(schema, item):
    data = copy.deepcopy(item._projection or item)
    update_serial_recursive(schema, item, data)
    return data

def get_serial_list(schema, items):
    return [get_serial_dict(schema, x) for x in items]
    
    
def update_serial_recursive(schema, item, data):
    for key in data.keys():    
        if is_object(schema[key]):
            update_serial_recursive(schema[key]['schema'], item[key], data[key])
        elif is_list_of_objects(schema[key]):
            for i in range(len(data[key])):
                update_serial_recursive(schema[key]['schema']['schema'], item[key][i], data[key][i])   
            
    for key in data.keys():
        if schema[key]['type'] == 'datetime':
            data[key] = data[key].isoformat()
        if schema[key]['type'] == 'reference':
            if hasattr(item[key], '__schema'):
                data[key] = get_serial_dict(item[key].__schema, item[key])
            else:
                data[key] = {'_err': 'reference not found'}
                

    for key in schema.keys():
        if 'serialize' in schema[key]:
            data[key] = schema[key]['serialize'](item)

