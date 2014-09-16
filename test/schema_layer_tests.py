import os
from unittest import TestCase
import datetime

import mongomock
from schemongo import schema_layer
from schemongo.db_layer.db_doc import DBDoc

import json
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
                "name": {"type":"string", "auto_init": lambda elem: elem.get_root().name.upper()},
                "edit": {"type":"integer", "auto": lambda elem: (elem.edit or 0) + 1},
            }}},
            "alt": {"type":"integer", "serialize": lambda elem: (elem.edit or 0) + 1},
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
            "doclist": [{"_id": 1, "name": "BOB", "edit": 1}]
        })
        
        data = {
            "_id": 1,
            "name": "Bob2",
            "subdoc": {
                "_id": 1,
                "data": 2
            },
            "doclist": [{"_id": 1, "name": "Fred", "edit": 10}],
            "alt": 2
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
            "doclist": [{"_id": 1, "name": "BOB", "edit": 2}]
        })
        

    def test_auto(self):
        self.db.register_schema('test', {
            "name": {"type": "string"},
            "capname": {"type": "string", "auto": lambda elem: elem.get_root().name.upper()},
            "capnameinit": {"type": "string", "auto_init": lambda elem: elem.get_root().name.upper()},
        })

        data = {
            "name": "Bob",
        }
        errs = self.db.test.insert(data)
        self.assertIsNone(errs)

        data = self.db.test.find_one({"_id":1})
        self.assertEqual(data, {
            "_id": 1,
            "name": "Bob",
            "capname": "BOB",
            "capnameinit": "BOB",
        })

        errs = self.db.test.update({"_id":1, "name":"Fred"})
        self.assertIsNone(errs)

        data = self.db.test.find_one({"_id":1})
        self.assertEqual(data, {
            "_id": 1,
            "name": "Fred",
            "capname": "FRED",
            "capnameinit": "BOB",
        })


    def test_default(self):
        self.db.register_schema('test', {
            "name": {"type": "string"},
            "data": {"type": "string", "default": "Howdy"},
        })

        data = {
            "name": "Bob",
        }
        errs = self.db.test.insert(data)
        self.assertIsNone(errs)

        data = self.db.test.find_one({"_id":1})
        self.assertEqual(data, {
            "_id": 1,
            "name": "Bob",
            "data": "Howdy",
        })

        data = {
            "name": "Fred",
            "data": "Goodbye"
        }
        errs = self.db.test.insert(data)
        self.assertIsNone(errs)

        data = self.db.test.find_one({"_id":2})
        self.assertEqual(data, {
            "_id": 2,
            "name": "Fred",
            "data": "Goodbye"
        })


    def test_allowed(self):
        self.db.register_schema('test', {
            "name": {"type": "string", "allowed": ["Fred", "George"]},                
            "data": {"type": "integer"},
            "data2": {"type": "integer", "allowed": lambda elem: [elem.data-1,elem.data,elem.data+1]},
        })

        data = {
            "name": "Bob",
            "data": 4,
            "data2": 8
        }
        errs = self.db.test.insert(data)
        self.assertEqual(errs, ["name: 'Bob' not one of allowed values", "data2: '8' not one of allowed values"])

        data = {
            "name": "Fred",
            "data": 4,
            "data2": 5
        }
        errs = self.db.test.insert(data)
        self.assertIsNone(errs)

        data = {
            "_id": 1,
            "name": "Bob",
            "data": 4,
            "data2": 3
        }
        errs = self.db.test.update(data)
        self.assertEqual(errs, ["name: 'Bob' not one of allowed values"])


    def test_required(self):
        self.db.register_schema('test', {
            "name": {"type": "string"},                
            "data": {"type": "datetime"},
            "data2": {"type": "integer", "required": True},
        })

        data = {
            "name": "Bob",
            "data": '2011-04-05',
        }
        errs = self.db.test.insert(data)
        self.assertEqual(errs, ["data2: value is required"])

        data = {
            "name": "Fred",
            "data": '2011-04-05',
            "data2": 5
        }
        errs = self.db.test.insert(data)
        self.assertIsNone(errs)

        data = {
            "_id": 1,
            "name": "Fred",
            "data": '2011-04-05',
            "data2": None
        }
        errs = self.db.test.update(data)
        self.assertEqual(errs, ["data2: value is required"])


    def test_unique(self):
        self.db.register_schema('test', {
            "name": {"type": "string", "unique": True},                
            "data": {"type": "integer"},
            "data2": {"type": "integer", "required": True},
        })

        data = {
            "name": "Fred",
            "data": 4,
            "data2": 5
        }
        errs = self.db.test.insert(data)
        self.assertIsNone(errs)

        data = {
            "name": "Fred",
            "data": 5,
            "data2": 6
        }
        errs = self.db.test.insert(data)
        self.assertEqual(errs, ["name: 'Fred' is not unique"])
        
        data = {
            "name": "Bob",
            "data": 5,
            "data2": 6
        }
        errs = self.db.test.insert(data)
        self.assertIsNone(errs)

        data = {
            "_id": 2,
            "name": "Fred",
            "data": 5,
            "data2": 6
        }
        errs = self.db.test.update(data)
        self.assertEqual(errs, ["name: 'Fred' is not unique"])
        

    def test_serialize(self):
        self.db.register_schema('test', {
            "name": {"type": "string", 'required': True, 'unique': True},
            "subdoc": {"type": "dict", "schema": {
                "data": {"type":"integer", 'default': 3, 'read_only': True},
                "sdata": {'type': 'integer', 'serialize': lambda elem: elem.data + 1},
            }},
            "doclist": {"type": "list", "schema": {"type": "dict", "schema": {
                "name": {"type":"string", 'allowed': ['Fred', 'George']},
                "caps": {"type":"string", 'auto': lambda elem: elem.name.upper()},
                "init": {"type":"string", 'auto_init': lambda elem: elem.get_parent().get_root().name.lower()},
                "really": {'type': 'string', 'serialize': lambda elem: elem.name + ', really'},                    
            }}},            
        })

        data = {
            "name": "Bob",
            "doclist": [{'name': 'Fred'}]
        }
        errs = self.db.test.insert(data)
        self.assertIsNone(errs)
        
        inst = self.db.test.find_one({'_id':1})

        text = self.db.test.serialize(inst)
        data = json.loads(text)
        self.assertEqual(data, {
            "_id": 1,
            "name": "Bob",
            "subdoc": {
                "_id": 1,
                "data": 3,
                "sdata": 4,
            },
            "doclist": [{
                "_id": 1,
                "name": 'Fred',
                "caps": 'FRED',
                "init": 'bob',
                "really": 'Fred, really'
            }],
        })

        inst = self.db.test.find_one({'_id':1}, fields=['name', 'subdoc'])

        text = self.db.test.serialize(inst)
        data = json.loads(text)
        self.assertEqual(data, {
            "_id": 1,
            "name": "Bob",
            "subdoc": {
                "_id": 1,
                "data": 3,
                "sdata": 4,
            },
        })


    def test_datetime(self):
        self.db.register_schema('test', {
            "name": {"type": "string"},                
            "data": {"type": "datetime"},
        })

        data = {
            "name": "Bob",
            "data": '2011-04-05',
        }
        errs = self.db.test.insert(data)
        self.assertIsNone(errs)

        data = {
            "name": "Fred",
            "data": datetime.datetime(2011,04,05),
        }
        errs = self.db.test.insert(data)
        self.assertIsNone(errs)
        
        self.assertEqual(self.db.test.find_one({'_id':1}).data, datetime.datetime(2011,04,05))
        self.assertEqual(self.db.test.find_one({'_id':2}).data, datetime.datetime(2011,04,05))

        inst = self.db.test.find_one({'_id':2})
        text = self.db.test.serialize(inst)
        data = json.loads(text)
        self.assertEqual(data['data'], '2011-04-05T00:00:00')
        
