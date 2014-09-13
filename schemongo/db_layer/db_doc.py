#!/usr/bin/env python

class DBDoc(dict):
    def __init__(self, raw, parent=None, projection=None):
        dict.__init__(self, raw)
        self._parent = parent
        self._projection = projection
        
        for key, val in self.items():
            if isinstance(val, dict):
                self[key] = DBDoc(val, self)
            elif isinstance(val, list) and len(val) and isinstance(val[0], dict):
                self[key] = DBDocList(val, self)
        
    def __getattr__(self, key):
        if key not in self:
            raise AttributeError, key
        return self[key]
    
    def get_parent(self):
        return self._parent
    
    def get_root(self):
        current = self
        while current._parent:
            current = current._parent
        return current



class DBDocList(list):
    def __init__(self, raw, parent=None):
        list.__init__(self)
        self._parent = parent

        for item in raw:
            self.append(DBDoc(item, self))

    def get_parent(self):
        return self._parent
    
    def get_root(self):
        current = self
        while current._parent:
            current = current._parent
        return current



        
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
        elif isinstance(val, dict):
            original[key] = DBDoc(val, original)
        elif isinstance(val, list) and key in original and isinstance(original[key], list) and len(val) and isinstance(val[0], dict):
            old = original[key]
            ids = [x['_id'] for x in old]
            original[key] = DBDocList([], original)
            for doc in val:
                if '_id' in doc and doc['_id'] in ids:
                    new = old[ids.index(doc['_id'])]
                    merge(new, doc)
                    original[key].append(DBDoc(new, original[key]))
                else:
                    original[key].append(DBDoc(doc, original[key]))
        else:
            original[key] = val


