#!/usr/bin/env python

class DBLayerDoc(dict):
    def __init__(self, raw, projection=None):
        dict.__init__(self, raw)
        setattr(self, '_projection', projection)
        
        
def enforce_ids(item, _id):
    if isinstance(item, dict):
        for val in item.values():
            enforce_ids(val, 1)
        if '_id' not in item:
            item['_id'] = _id
            return _id + 1
    elif isinstance(item, list):
        new_id = 1
        if len(item) and isinstance(item[0], dict):
            new_id = max(x.get('_id',0) for x in item)+1
        for val in item:
            new_id = enforce_ids(val, new_id)
    return _id        



def merge(original, new):
    for key, val in new.items():
        if isinstance(val, dict) and key in original and isinstance(original[key], dict):
            merge(original[key], val)
        elif isinstance(val, list) and key in original and isinstance(original[key], list) and len(val) and isinstance(val[0], dict):
            old = original[key]
            ids = [x['_id'] for x in old]
            original[key] = []
            for doc in val:
                if '_id' in doc and doc['_id'] in ids:
                    new = old[ids.index(doc['_id'])]
                    merge(new, doc)
                    original[key].append(new)
                else:
                    original[key].append(doc)
        else:
            original[key] = val















