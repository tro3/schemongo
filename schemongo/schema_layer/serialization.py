#!/usr/bin/env python

import json
from schema_doc import is_object, is_list_of_objects

def serialize(schema, item):
    data = item._projection or item
    update_serial(schema, data)
    return json.dumps(data)
    
    
def update_serial(schema, data):
    for key in data.keys():    
        if is_object(schema[key]):
            serialize(schema[key]['schema'], data[key])
        elif is_list_of_objects(schema[key]):
            [serialize(schema[key]['schema']['schema'], x) for x in data[key]]
            
    for key in schema.keys():
        if 'serialize' in schema[key]:
            data[key] = schema[key]['serialize'](data)


