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
        
        
    def test_basics(self):
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

        data = {
            "name": "Bob",
            "subdoc": {
                "data": 4
            },
            "hash": {4:5},
            "num_list": [4,5],
            "doclist": [{"name": "Fred"}]
        }
        errs = self.db.test.insert(data)
        self.assertIsNone(errs)
        
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

        errs = self.db.test.update({"_id":1, "name":"Bob2"})
        self.assertIsNone(errs)
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


    def test_datatype_errors(self):
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

        data = {
            "name": "Bob",
            "subdoc": {
                "data": "fred"
            },
            "hash": {4:5},
            "num_list": [4,5],
            "doclist": [{"name": "Fred"}]
        }
        errs = self.db.test.insert(data)
        self.assertEqual(errs, ["subdoc/data: Could not convert 'fred' to type 'integer'"])

        data['subdoc']['data'] = 1
        errs = self.db.test.insert(data)
        self.assertIsNone(errs)

        errs = self.db.test.update({"_id":1, "subdoc":{"data":"fred"}})
        self.assertEqual(errs, ["subdoc/data: Could not convert 'fred' to type 'integer'"])


    def test_readonly(self):
        self.db.register_schema('test', {
            "name": {"type": "string", "read_only": True},
            "subdoc": {"type": "dict", "schema": {
                "data": {"type":"integer"}
            }},
            "hash": {"type": "dict"},
            "num_list": {"type": "list", "schema": {"type": "integer"}},
            "doclist": {"type": "list", "schema": {"type": "dict", "schema": {
                "name": {"type":"string", "auto_init": lambda elem: elem.get_root().name.upper()}
            }}},            
        })

        data = {
            "name": "Bob",
            "subdoc": {
                "data": 1
            },
            "hash": {4:5},
            "num_list": [4,5],
            "doclist": [{}]
        }
        errs = self.db.test.insert(data, direct=True)
        self.assertIsNone(errs)

        data = self.db.test.find_one({"_id":1})
        self.assertEqual(data, {
            "_id": 1,
            "name": "Bob",
            "subdoc": {
                "_id": 1,
                "data": 1
            },
            "hash": {"_id": 1, 4:5},
            "num_list": [4,5],
            "doclist": [{"_id": 1, "name": "BOB"}]
        })
        
        data = {
            "_id": 1,
            "name": "Bob2",
            "subdoc": {
                "_id": 1,
                "data": 2
            },
            "doclist": [{"_id": 1, "name": "Fred"}]
        }
        errs = self.db.test.update(data)
        self.assertIsNone(errs)

        data = self.db.test.find_one({"_id":1})
        self.assertEqual(data, {
            "_id": 1,
            "name": "Bob",
            "subdoc": {
                "_id": 1,
                "data": 2
            },
            "hash": {"_id": 1, 4:5},
            "num_list": [4,5],
            "doclist": [{"_id": 1, "name": "BOB"}]
        })
        
