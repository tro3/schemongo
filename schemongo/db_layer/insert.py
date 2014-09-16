
import datetime
from connection import db


def get_maxid(collection):
    db_collection = db[collection]
    return db_collection.find().count() and db_collection.find().sort('_id',-1).limit(1)[0]['_id']


def add_ids_recursive(items, max_id=None):
    assert isinstance(items, list)
    if max_id is None:
        max_id = max(x.get('_id', 0) for x in items)
    for item in items:
        if '_id' not in item or not item['_id']:
            max_id += 1
            item['_id'] = max_id
        search_object_recursive(item)
            

def search_object_recursive(obj):
    assert isinstance(obj, dict)
    for key, val in obj.items():
        if isinstance(val, list) and len(val)>0 and isinstance(val[0], dict):
            add_ids_recursive(val)
        if isinstance(val, dict):
            if '_id' not in val or not val['_id']:
                val['_id'] = 1
            search_object_recursive(val)



def insert(collection, user, doc_or_docs):
    db_collection = db[collection]
    if isinstance(doc_or_docs, dict):
        docs = [doc_or_docs]
    elif isinstance(doc_or_docs, list):
        docs = doc_or_docs
    else:
        raise TypeError, doc_or_docs
    
    max_id = db_collection.find().count() and db_collection.find().sort('_id',-1).limit(1)[0]['_id']
    add_ids_recursive(docs, max_id)
        
    tmp = db_collection.insert(docs)
    for doc in docs:
        get_collection('_history').insert({
            'collection': collection,
            'item':  doc['_id'],
            'log': [
                {
                    'time': datetime.datetime.now(),
                    'user': user,
                    'action': 'document created'
                }
            ]
        })
        
    return max_id+1
    
