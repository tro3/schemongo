import os
from unittest import TestCase
import datetime

import mongomock
from schemongo import schema_layer
from schemongo.db_layer.db_doc import DBDoc

from pprint import pprint as p


class SchemaLayerTests(TestCase):
    
    def setUp(self):
        self.db = schema_layer.init(mongomock.MongoClient())
        
        self.db.register_schema('test', {
            "name": {"type": "string"},
            "subdoc": {"type": "dict", "schema": {
                "data": {"type":"integer"}
            }},
            "hash": {"type": "dict"},
            "num_list": {"type": "list", "schema": {"type": "integer"}},
            "doclist": {"type": "list", "schema": {"type": "dict", "schema": {
                "name": {"type":"string"}
            }}},            
        })

        
    def test_basics(self):
        data = {
            "name": "Bob",
            "subdoc": {
                "data": 4
            },
            "hash": {4:5},
            "num_list": [4,5],
            "doclist": [{"name": "Fred"}]
        }
        self.db.test.insert(data)
        
        data = self.db.test.find_one({"name":"Bob"})
        self.assertEqual(type(data), DBDoc)
        self.assertEqual(data, {
            "_id": 1,
            "name": "Bob",
            "subdoc": {
                "_id": 1,
                "data": 4
            },
            "hash": {"_id": 1, 4:5},
            "num_list": [4,5],
            "doclist": [{"_id": 1, "name": "Fred"}]
        })

        self.db.test.update({"_id":1, "name":"Bob2"})
        data = self.db.test.find_one({"_id":1})
        self.assertEqual(type(data), DBDoc)
        self.assertEqual(data, {
            "_id": 1,
            "name": "Bob2",
            "subdoc": {
                "_id": 1,
                "data": 4
            },
            "hash": {"_id": 1, 4:5},
            "num_list": [4,5],
            "doclist": [{"_id": 1, "name": "Fred"}]
        })

    
