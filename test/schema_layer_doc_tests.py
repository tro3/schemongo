import os
from unittest import TestCase
import datetime

import mongomock
from schemongo.schema_layer.schema_doc import DBDoc, DBDocList, \
    enforce_schema, generate_prototype, run_auto_funcs, enforce_ids, merge

from pprint import pprint as p


class DBDocTests(TestCase):
    
    def test_type_propagation(self):
        schema = {
            "_id": {"type": "integer"},
            "name": {"type": "string"},
            "subdoc": {"type": "dict", "schema": {
                "data": {"type":"integer"}
            }},
            "hash": {"type": "dict"},
            "num_list": {"type": "list", "schema": {"type": "integer"}},
            "doclist": {"type": "list", "schema": {"type": "dict", "schema": {
                "name": {"type":"string"}
            }}},            
        }
        data = DBDoc({
            "name":"bob",
            "subdoc": {
                "data": 1
            },
            "hash": {
                "data": 1,    
            },
            "num_list": [1,2],
            "doclist": [
                {"name": "fred"},
                {"name": "george"},
            ]
        })
        
        self.assertIsInstance(data.name, str)
        self.assertIsInstance(data['name'], str)
        self.assertIsInstance(data.subdoc, DBDoc)
        self.assertIsInstance(data['subdoc'], DBDoc)
        self.assertIsInstance(data.hash, DBDoc)
        self.assertIsInstance(data['hash'], DBDoc)
        self.assertIsInstance(data['hash']['data'], int)
        self.assertIsInstance(data.num_list, list)
        self.assertIsInstance(data['num_list'], list)
        self.assertIsInstance(data.doclist, DBDocList)
        self.assertIsInstance(data['doclist'], DBDocList)
        self.assertIsInstance(data.doclist[0], DBDoc)
        self.assertIsInstance(data['doclist'][0], DBDoc)


    def test_enforce_schema(self):
        schema = {
            "_id": {"type": "integer"},
            "name": {"type": "string"},
            "key": {"type": "string", "read_only": True},
            "subdoc": {"type": "dict", "schema": {
                "data": {"type":"integer"}
            }},
            "hash": {"type": "dict"},
            "num_list": {"type": "list", "schema": {"type": "integer"}},
            "doclist": {"type": "list", "schema": {"type": "dict", "schema": {
                "name": {"type":"string"}
            }}},            
        }
        data = {
            "name":"bob",
            "name2": "fred",
            "subdoc": {
                "data": "1"
            },
            "hash": {
                "data1": 1,    
            },
            "num_list": ["1","2"],
            "doclist": [
                {"name": "fred"},
                {"name": "george"},
            ]
        }        
        enforce_schema(schema, data)
        
        self.assertIsInstance(data['name'], str)
        self.assertFalse('name2' in data)
        self.assertFalse('key' in data)
        self.assertIsInstance(data['subdoc']['data'], int)
        self.assertIsInstance(data['hash'], dict)
        self.assertIsInstance(data['hash']['data1'], int)
        self.assertIsInstance(data['num_list'], list)
        self.assertIsInstance(data['num_list'][0], int)
        self.assertIsInstance(data['doclist'][0]['name'], str)



    def test_prototype(self):
        schema = {
            "name": {"type": "string"},
            "key": {"type": "string", "read_only": True},
            "key2": {"type": "string", "default": "Fred"},
            "subdoc": {"type": "dict", "schema": {
                "data": {"type":"integer"}
            }},
            "hash": {"type": "dict"},
            "num_list": {"type": "list", "schema": {"type": "integer"}},
            "doclist": {"type": "list", "schema": {"type": "dict", "schema": {
                "name": {"type":"string"}
            }}},            
        }
        inst = generate_prototype(schema)
        self.assertEqual(inst, {            
            "name":None,
            "key": None,
            "key2": "Fred",
            "subdoc": {
                "data": None
            },
            "hash": {},
            "num_list": [],
            "doclist": []
        })


    def test_auto(self):
        schema = {
            "_id": {"type": "integer"},
            "name": {"type": "string"},
            "key": {"type": "string", "auto": lambda elem: elem.name.upper()},
            "key2": {"type": "string", "auto": lambda elem: elem.name.upper()},
            "subdoc": {"type": "dict", "schema": {
                "data": {"type":"integer"}
            }},
            "hash": {"type": "dict"},
            "num_list": {"type": "list", "schema": {"type": "integer"}},
            "doclist": {"type": "list", "schema": {"type": "dict", "schema": {
                "name": {"type":"string"},
                "title": {"type":"string", "auto_init": lambda elem: elem.get_parent().get_parent().name.upper()}
            }}},            
        }
        data = DBDoc({
            "name":"bob",
            "key":"fred",
            "subdoc": {
                "data": 1
            },
            "hash": {
                "data": 1,    
            },
            "num_list": [1,2],
            "doclist": [
                {"_id":1, "name": "fred", "title": "FRED"},
                {"name": "george"},
            ]
        })
        
        run_auto_funcs(schema, data)
    
        self.assertEqual(data, {
            "name":"bob",
            "key":"BOB",
            "key2":"BOB",
            "subdoc": {
                "data": 1
            },
            "hash": {
                "data": 1,    
            },
            "num_list": [1,2],
            "doclist": [
                {"_id":1, "name": "fred", "title": "FRED"},
                {"name": "george", "title": "BOB"},
            ]
        })


    def test_write_flow(self):
        schema = {
            "_id": {"type": "integer"},
            "name": {"type": "string"},
            "key": {"type": "string", "auto": lambda elem: elem.name.upper()},
            "key2": {"type": "string", "auto": lambda elem: elem.name.upper()},
            "subdoc": {"type": "dict", "schema": {
                "data": {"type":"integer"}
            }},
            "hash": {"type": "dict"},
            "num_list": {"type": "list", "schema": {"type": "integer"}},
            "doclist": {"type": "list", "schema": {"type": "dict", "schema": {
                "_id": {"type": "integer"},
                "name": {"type":"string"},
                "title": {"type":"string", "auto_init": lambda elem: elem.get_root().name.upper()}
            }}},            
        }
        data = DBDoc({
            "name":"fred",
            "key":"FRED",
            "hash": {
                "data": 1,    
            },
            "num_list": [1,2],
            "doclist": [
                {"_id":1, "name": "fred", "title": "GEORGE"},
            ]
        })
        incoming = {
            "name": "bob",
            "subdoc": {
                "data": "1"
            },
            "doclist": [
                {"_id":1},
                {"name": "amber"},
            ]            
        }
        enforce_schema(schema, incoming)
        merge(data, incoming)
        run_auto_funcs(schema, data)
        enforce_ids(data, 10)
        
        self.assertEqual(data, {
            "_id": 10,
            "name":"bob",
            "key":"BOB",
            "key2":"BOB",
            "subdoc": {
                "_id": 1,
                "data": 1
            },
            "hash": {
                "_id": 1,
                "data": 1,    
            },
            "num_list": [1,2],
            "doclist": [
                {"_id":1, "name": "fred", "title": "GEORGE"},
                {"_id":2, "name": "amber", "title": "BOB"},
            ]
        })











