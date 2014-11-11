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
        ids, errs = self.db.test.insert(data)
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
        ids, errs = self.db.test.insert(data)
        self.assertEqual(errs, ["subdoc/data: Could not convert 'fred' to type 'integer'"])

        data['subdoc']['data'] = 1
        ids, errs = self.db.test.insert(data)
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
        ids, errs = self.db.test.insert(data, direct=True)
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
        ids, errs = self.db.test.insert(data)
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
        ids, errs = self.db.test.insert(data)
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
        ids, errs = self.db.test.insert(data)
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
        ids, errs = self.db.test.insert(data)
        self.assertEqual(errs, ["name: 'Bob' not one of the allowed values", "data2: '8' not one of the allowed values"])

        data = {
            "name": "Fred",
            "data": 4,
            "data2": 5
        }
        ids, errs = self.db.test.insert(data)
        self.assertIsNone(errs)

        data = {
            "_id": 1,
            "name": "Bob",
            "data": 4,
            "data2": 3
        }
        errs = self.db.test.update(data)
        self.assertEqual(errs, ["name: 'Bob' not one of the allowed values"])


    def test_allowed_list(self):
        self.db.register_schema('test', {
            "name": {"type": "string", "allowed": ["Fred", "George"]},                
            "data": {"type": "integer"},
            "data2": {"type": 'list', 'schema': 
                {"type": "integer", "allowed": lambda elem: [elem.data-1,elem.data,elem.data+1]},
            }
        })

        data = {
            "name": "Bob",
            "data": 4,
            "data2": [8]
        }
        ids, errs = self.db.test.insert(data)
        self.assertEqual(errs, ["name: 'Bob' not one of the allowed values", "data2/0: '8' not one of the allowed values"])

        data = {
            "name": "Fred",
            "data": 4,
            "data2": [5]
        }
        ids, errs = self.db.test.insert(data)
        self.assertIsNone(errs)

        data = {
            "_id": 1,
            "name": "Fred",
            "data": 4,
            "data2": [3,8]
        }
        errs = self.db.test.update(data)
        self.assertEqual(errs, ["data2/1: '8' not one of the allowed values"])


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
        ids, errs = self.db.test.insert(data)
        self.assertEqual(errs, ["data2: value is required"])

        data = {
            "name": "Fred",
            "data": '2011-04-05',
            "data2": 5
        }
        ids, errs = self.db.test.insert(data)
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
        ids, errs = self.db.test.insert(data)
        self.assertIsNone(errs)

        data = {
            "name": "Fred",
            "data": 5,
            "data2": 6
        }
        ids, errs = self.db.test.insert(data)
        self.assertEqual(errs, ["name: 'Fred' is not unique"])
        
        data = {
            "name": "Bob",
            "data": 5,
            "data2": 6
        }
        ids, errs = self.db.test.insert(data)
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
                "data": {"type":"string"},
                "caps": {"type":"string", 'auto': lambda elem: elem.name.upper()},
                "init": {"type":"string", 'auto_init': lambda elem: elem.get_parent().get_root().name.lower()},
                "really": {'type': 'string', 'serialize': lambda elem: elem.name + ', really'},                    
            }}},            
        })

        data = {
            "name": "Bob",
            "doclist": [{'name': 'Fred'}]
        }
        ids, errs = self.db.test.insert(data)
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
                "data": None,
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
        ids, errs = self.db.test.insert(data)
        self.assertIsNone(errs)

        data = {
            "name": "Fred",
            "data": datetime.datetime(2011,04,05),
        }
        ids, errs = self.db.test.insert(data)
        self.assertIsNone(errs)
        
        self.assertEqual(self.db.test.find_one({'_id':1}).data, datetime.datetime(2011,04,05))
        self.assertEqual(self.db.test.find_one({'_id':2}).data, datetime.datetime(2011,04,05))

        inst = self.db.test.find_one({'_id':2})
        text = self.db.test.serialize(inst)
        data = json.loads(text)
        self.assertEqual(data['data'], '2011-04-05T00:00:00')
        
        data = {
            "name": "Bob",
            "data": 'fred',
        }
        ids, errs = self.db.test.insert(data)
        self.assertEqual(errs, ["data: Could not convert 'fred' to type 'datetime'"])


    def test_bulk_insert(self):
        self.db.register_schema('test', {
            "name": {"type": "string"},                
            "data": {"type": "integer"},
        })

        data = [
            {"name": "Bob", "data": '2011'},
            {"name": "Fred", "data": '2012'},
        ]
        ids, errs = self.db.test.insert(data)
        self.assertIsNone(errs)
        self.assertEqual(self.db.test.find({}).count(), 2)


    def test_direct_update(self):
        self.db.register_schema('test', {
            "name": {"type": "string"},                
            "data": {"type": "integer", "read_only": True},
        })

        data = [
            {"name": "Bob", "data": 1},
            {"name": "Fred", "data": 2},
        ]
        ids, errs = self.db.test.insert(data)
        self.assertIsNone(errs)
        
        self.assertEqual(self.db.test.find({'_id':1})[0].data, None)
        data = {"_id":1, "name": "Bob", "data": 1}
        errs = self.db.test.update(data, direct=True)
        self.assertEqual(self.db.test.find({'_id':1})[0].data, 1)
        
        
    def test_datatypes(self, ):
        self.db.register_schema('test', {
            "name": {"type": "string"},                
            "bool": {"type": "boolean"},
            "int": {"type": "integer"},
            "float": {"type": "float"},
            "list": {"type": "list"},
            "hash": {"type": "dict"},
            "datetime": {"type": "datetime"},
        })

        data = {
            'name': 'Bob',
            'bool': True,
            'int': 4,
            'float': '2.34',
            'list': [2, 'a'],
            'hash': {'a': 2},
            'datetime': '2011-01-01',
        }
        ids, errs = self.db.test.insert(data)
        self.assertIsNone(errs)

        data = self.db.test.find_one({'_id':1})
        self.assertEqual(data, {
            '_id': 1,
            'name': 'Bob',
            'bool': True,
            'int': 4,
            'float': 2.34,
            'list': [2, 'a'],
            'hash': {'a': 2, '_id': 1},
            'datetime': datetime.datetime(2011,1,1)
        })

        data = json.loads(self.db.test.serialize(data))
        self.assertEqual(data, {
            '_id': 1,
            'name': 'Bob',
            'bool': True,
            'int': 4,
            'float': 2.34,
            'list': [2, 'a'],
            'hash': {'a': 2, '_id': 1},
            'datetime': '2011-01-01T00:00:00',
        })


    def test_references(self):
        self.db.register_schema('users', {
            "username": {"type": "string"},
            "group": {"type": "string"},
            "location": {"type": "string"},
        })
        self.db.register_schema('test', {
            "name": {"type": "string"},
            "contact": {
                'type': 'reference',
                'collection': 'users',
                'fields': ['username', 'location'],
            }
        })
        
        ids, errs = self.db.users.insert([
            {'username':'bob', 'group':'Sales', 'location': 'Paris'},
            {'username':'fred', 'group':'Sales', 'location': 'Caen'},
        ])
        self.assertIsNone(errs)
        self.assertEqual(ids, [1,2])
        
        data = {
            'name': 'Samsung',
            'contact': {
                '_id': 1,
                'username': 'bob'
            }
        }
        ids, errs = self.db.test.insert(data)
        self.assertIsNone(errs)
        
        inst = self.db.test.find_one({'_id':1})
        self.assertEqual(inst, {
            '_id': 1,
            'name': 'Samsung',
            'contact': {
                '_id': 1,
                'username': 'bob',
                'group': 'Sales',
                'location': 'Paris'
            }
        })
        self.assertEqual(inst.contact._projection, {
            '_id': 1,
            'username': 'bob',
            'location': 'Paris'
        })

        data = json.loads(self.db.test.serialize(inst))
        self.assertEqual(data, {
            '_id': 1,
            'name': 'Samsung',
            'contact': {
                '_id': 1,
                'username': 'bob',
                'location': 'Paris'
            }
        })

        data = {
            '_id': 1,
            'contact': {
                '_id': 2,
            }
        }
        errs = self.db.test.update(data)
        self.assertIsNone(errs)

        inst = self.db.test.find({'_id':1})[0]
        data = json.loads(self.db.test.serialize(inst))
        self.assertEqual(data, {
            '_id': 1,
            'name': 'Samsung',
            'contact': {
                '_id': 2,
                'username': 'fred',
                'location': 'Caen'
            }
        })


    def test_serialized_references(self):
        self.db.register_schema('users', {
            "first_name": {"type": "string"},
            "last_name": {"type": "string"},
            "full_name": {'type': 'string', 'serialize': lambda e: '%s %s' % (e.first_name, e.last_name)}
        })
        self.db.register_schema('test', {
            "name": {"type": "string"},
            "contact": {
                'type': 'reference',
                'collection': 'users',
                'fields': ['full_name'],
            }
        })

        ids, errs = self.db.users.insert([
            {'first_name':'Bob', 'last_name': 'Paris'},
            {'first_name':'Fred', 'last_name': 'Caen'},
        ])
        self.assertIsNone(errs)
        
        data = {
            'name': 'Samsung',
            'contact': {
                '_id': 1,
                'full_name': 'Bob Paris'
            }
        }
        ids, errs = self.db.test.insert(data)
        self.assertIsNone(errs)

        inst = self.db.test.find_one({'_id':1})
        data = json.loads(self.db.test.serialize(inst))
        self.assertEqual(data, {
            '_id': 1,
            'name': 'Samsung',
            'contact': {
                '_id': 1,
                'full_name': 'Bob Paris'
            }
        })


    def test_serialized_reference_list(self):
        self.db.register_schema('users', {
            "first_name": {"type": "string"},
            "last_name": {"type": "string"},
            "full_name": {'type': 'string', 'serialize': lambda e: '%s %s' % (e.first_name, e.last_name)}
        })
        self.db.register_schema('test', {
            "name": {"type": "string"},
            "contacts": {"type": "list", "schema": {
                'type': 'reference',
                'collection': 'users',
                'fields': ['full_name'],
            }}
        })

        ids, errs = self.db.users.insert([
            {'first_name':'Bob', 'last_name': 'Paris'},
            {'first_name':'Fred', 'last_name': 'Caen'},
        ])
        self.assertIsNone(errs)
        
        data = {
            'name': 'Samsung',
            'contacts': [{
                '_id': 1,
                'full_name': 'Bob Paris'
            }]
        }
        ids, errs = self.db.test.insert(data)
        self.assertIsNone(errs)

        inst = self.db.test.find_one({'_id':1})
        data = json.loads(self.db.test.serialize(inst))
        self.assertEqual(data, {
            '_id': 1,
            'name': 'Samsung',
            'contacts': [{
                '_id': 1,
                'full_name': 'Bob Paris'
            }]
        })


    def test_delete_singular_reference(self):
        self.db.register_schema('users', {
            "first_name": {"type": "string"},
            "last_name": {"type": "string"},
            "full_name": {'type': 'string', 'serialize': lambda e: '%s %s' % (e.first_name, e.last_name)}
        })
        self.db.register_schema('test', {
            "name": {"type": "string"},
            "contact": {
                'type': 'reference',
                'collection': 'users',
                'fields': ['full_name'],
            }
        })

        ids, errs = self.db.users.insert([
            {'first_name':'Bob', 'last_name': 'Paris'},
            {'first_name':'Fred', 'last_name': 'Caen'},
        ])
        self.assertIsNone(errs)
        
        data = {
            'name': 'Samsung',
            'contact': {'_id': 1}
        }
        ids, errs = self.db.test.insert(data)
        self.assertIsNone(errs)

        errs = self.db.users.remove({'_id': 1})
        self.assertIsNone(errs)

        inst = self.db.test.find_one({'_id':1})
        data = json.loads(self.db.test.serialize(inst))
        self.assertEqual(data, {
            '_id': 1,
            'name': 'Samsung',
            'contact': None
        })


    def test_delete_required_singular_reference(self):
        self.db.register_schema('users', {
            "first_name": {"type": "string"},
            "last_name": {"type": "string"},
            "full_name": {'type': 'string', 'serialize': lambda e: '%s %s' % (e.first_name, e.last_name)}
        })
        self.db.register_schema('test', {
            "name": {"type": "string"},
            "contact": {
                'type': 'reference',
                'collection': 'users',
                'fields': ['full_name'],
                'required': True,
            }
        })

        ids, errs = self.db.users.insert([
            {'first_name':'Bob', 'last_name': 'Paris'},
            {'first_name':'Fred', 'last_name': 'Caen'},
        ])
        self.assertIsNone(errs)
        
        data = {
            'name': 'Samsung',
            'contact': {'_id': 1}
        }
        ids, errs = self.db.test.insert(data)
        self.assertIsNone(errs)

        errs = self.db.users.remove({'_id': 1})
        self.assertIsNotNone(errs)

        self.assertEqual(errs, ["Collection 'test', item 1: undeleteable reference encountered"])


    def test_delete_reference_in_list(self):
        self.db.register_schema('users', {
            "first_name": {"type": "string"},
            "last_name": {"type": "string"},
            "full_name": {'type': 'string', 'serialize': lambda e: '%s %s' % (e.first_name, e.last_name)}
        })
        self.db.register_schema('test', {
            "name": {"type": "string"},
            "contacts": {"type": "list", "schema": {
                'type': 'reference',
                'collection': 'users',
                'fields': ['full_name'],
            }}
        })

        ids, errs = self.db.users.insert([
            {'first_name':'Bob', 'last_name': 'Paris'},
            {'first_name':'Fred', 'last_name': 'Caen'},
        ])
        self.assertIsNone(errs)
        
        data = {
            'name': 'Samsung',
            'contacts': [{'_id': 1},{'_id': 2}]
        }
        ids, errs = self.db.test.insert(data)
        self.assertIsNone(errs)

        errs = self.db.users.remove({'_id': 1})
        self.assertIsNone(errs)

        inst = self.db.test.find_one({'_id':1})
        data = json.loads(self.db.test.serialize(inst))
        self.assertEqual(data, {
            '_id': 1,
            'name': 'Samsung',
            'contacts': [{
                '_id': 2,
                'full_name': 'Fred Caen'
            }]
        })


    def test_serialized_projection(self):
        self.db.register_schema('test', {
            "first_name": {"type": "string"},
            "last_name": {"type": "string"},
            "full_name": {'type': 'string', 'serialize': lambda e: '%s %s' % (e.first_name, e.last_name)}
        })

        ids, errs = self.db.test.insert([
            {'first_name':'Bob', 'last_name': 'Paris'},
            {'first_name':'Fred', 'last_name': 'Caen'},
        ])
        self.assertIsNone(errs)

        inst = self.db.test.find_one({'_id':1}, fields=['full_name'])
        data = json.loads(self.db.test.serialize(inst))
        self.assertEqual(data, {
            '_id': 1,
            'full_name': 'Bob Paris'
        })


    def test_references_error(self):
        self.db.register_schema('users', {
            "first_name": {"type": "string"},
            "last_name": {"type": "string"},
            "full_name": {'type': 'string', 'serialize': lambda e: '%s %s' % (e.first_name, e.last_name)}
        })
        self.db.register_schema('test', {
            "name": {"type": "string"},
            "contact": {
                'type': 'reference',
                'collection': 'users',
                'fields': ['full_name'],
            }
        })

        ids, errs = self.db.users.insert([
            {'first_name':'Bob', 'last_name': 'Paris'},
            {'first_name':'Fred', 'last_name': 'Caen'},
        ])
        self.assertIsNone(errs)
        
        data = {
            'name': 'Samsung',
            'contact': {
                '_id': 3
            }
        }
        ids, errs = self.db.test.insert(data)
        self.assertIsNone(errs)

        inst = self.db.test.find_one({'_id':1})
        data = json.loads(self.db.test.serialize(inst))
        self.assertEqual(data, {
            '_id': 1,
            'name': 'Samsung',
            'contact': {
                '_err': 'reference not found'
            }
        })


    def test_update_prototype(self):
        self.db.register_schema('test', {
            "name": {"type": "string", 'required': True, 'unique': True},
            "subdoc": {"type": "dict", "schema": {
                "data": {"type":"integer", 'default': 3, 'read_only': True},
                "sdata": {'type': 'integer', 'serialize': lambda elem: elem.data + 1},
            }},
            "doclist": {"type": "list", "schema": {"type": "dict", "schema": {
                "name": {"type":"string", 'allowed': ['Fred', 'George']},
                "data": {"type":"string"},
                "caps": {"type":"string", 'auto': lambda elem: elem.name.upper()},
                "init": {"type":"string", 'auto_init': lambda elem: elem.get_parent().get_root().name.lower()},
                "really": {'type': 'string', 'serialize': lambda elem: elem.name + ', really'},                    
            }}},            
        })

        data = {
            "name": "Bob",
            "doclist": []
        }
        ids, errs = self.db.test.insert(data)
        self.assertIsNone(errs)

        data = {
            "_id": 1,
            "name": "Bob",
            "doclist": [{'name': 'George'}]
        }
        errs = self.db.test.update(data)
        self.assertIsNone(errs)
        

        data = json.loads(self.db.test.serialize(self.db.test.find_one({'_id':1})))
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
                "name": 'George',
                "data": None,
                "caps": 'GEORGE',
                "init": 'bob',
                "really": 'George, really'
            }],
        })
        


    def test_update_with_embedded_reference(self):
        self.db.register_schema('users', {
            "name": {"type": "string", 'required': True, 'unique': True},
        })        
        
        self.db.register_schema('test', {
            "name": {"type": "string", 'required': True, 'unique': True},
            "author": {"type": "reference", "collection": "users", "fields":["name"]},
            "comments": {"type": "list", "schema": {"type": "dict", "schema": {
                "text": {"type": "string"},
                "creator": {"type": "reference", "collection": "users", "fields":["name"]},
                "creatorId": {"type": "integer", "serialize": lambda e: e.creator._id},
            }}}
        })

        data = {
            "name": "Bob"
        }
        ids, errs = self.db.users.insert(data)
        self.assertIsNone(errs)

        data = {
            "name": "My Thesis",
            "author": {"_id":1},
            "comments": [
                {"text": "Yo", "creator": {"_id": 1}}
            ]
        }
        ids, errs = self.db.test.insert(data)
        self.assertIsNone(errs)

        data = {
            "_id": 1,
            "name": "My Thesis 2",
            "author": {"_id": 1},
            "comments": [
                {"_id": 1, "text": "Yo 2", "creator": {"_id": 1}}
            ]
        }
        errs = self.db.test.update(data)
        self.assertIsNone(errs)

        data = json.loads(self.db.test.serialize(self.db.test.find_one({'_id':1})))
        self.assertEqual(data, {
            "_id": 1,
            "name": "My Thesis 2",
            "author": {
                "_id":1,
                "name": "Bob",
            },
            "comments": [
                {
                    "_id": 1,
                    "text": "Yo 2",
                    "creatorId": 1,
                    "creator": {
                        "_id": 1,
                        "name": "Bob",                       
                    }
                }
            ],               
        })


    def test_update_with_auto_reference(self):
        self.db.register_schema('users', {
            "name": {"type": "string", 'required': True, 'unique': True},
        })        

        data = {
            "name": "Bob"
        }
        ids, errs = self.db.users.insert(data)
        self.assertIsNone(errs)
        bob = self.db.users.find_one(1)
        
        self.db.register_schema('test', {
            "name": {"type": "string", 'required': True, 'unique': True},
            "author": {"type": "reference", "collection": "users", "fields":["name"], "auto_init": lambda e: bob},
            "comments": {"type": "list", "schema": {"type": "dict", "schema": {
                "text": {"type": "string"},
                "creator": {"type": "reference", "collection": "users", "fields":["name"], "auto_init": lambda e: bob},
                "creatorId": {"type": "integer", "serialize": lambda e: e.creator._id},
            }}}
        })


        data = {
            "name": "My Thesis",
            "comments": [
                {"text": "Yo"}
            ]
        }
        ids, errs = self.db.test.insert(data)
        self.assertIsNone(errs)

        data = json.loads(self.db.test.serialize(self.db.test.find_one({'_id':1})))
        self.assertEqual(data, {
            "_id": 1,
            "name": "My Thesis",
            "author": {
                "_id":1,
                "name": "Bob",
            },
            "comments": [
                {
                    "_id": 1,
                    "text": "Yo",
                    "creatorId": 1,
                    "creator": {
                        "_id": 1,
                        "name": "Bob",                       
                    }
                }
            ],               
        })

        data = {
            "_id": 1,
            "name": "My Thesis 2",
            "author": {"_id": 1},
            "comments": [
                {"_id": 1, "text": "Yo 2"}
            ]
        }
        errs = self.db.test.update(data)
        self.assertIsNone(errs)

        data = json.loads(self.db.test.serialize(self.db.test.find_one({'_id':1})))
        self.assertEqual(data, {
            "_id": 1,
            "name": "My Thesis 2",
            "author": {
                "_id":1,
                "name": "Bob",
            },
            "comments": [
                {
                    "_id": 1,
                    "text": "Yo 2",
                    "creatorId": 1,
                    "creator": {
                        "_id": 1,
                        "name": "Bob",                       
                    }
                }
            ],               
        })


    def test_null_date(self):
        self.db.register_schema('test', {
            "name": {"type": "string"},
            "date": {"type": "datetime"},
        })

        data = {
            "name": "Bob"
        }
        ids, errs = self.db.test.insert(data)
        self.assertIsNone(errs)

        data = json.loads(self.db.test.serialize(self.db.test.find_one({'_id':1})))
        self.assertEqual(data, {
            "_id": 1,
            "name": "Bob",
            "date": None,
        })


    def test_null_reference(self):
        self.db.register_schema('users', {
            "name": {"type": "string", 'required': True, 'unique': True},
        })        
        
        self.db.register_schema('test', {
            "name": {"type": "string", 'required': True, 'unique': True},
            "author": {"type": "reference", "collection": "users", "fields":["name"]},
        })

        data = {
            "name": "Bob"
        }
        ids, errs = self.db.test.insert(data)
        self.assertIsNone(errs)

        data = json.loads(self.db.test.serialize(self.db.test.find_one({'_id':1})))
        self.assertEqual(data, {
            "_id": 1,
            "name": "Bob",
            "author": None,
        })


    def test_find_one_null_with_reference(self):
        self.db.register_schema('users', {
            "name": {"type": "string", 'required': True, 'unique': True},
        })        
        
        self.db.register_schema('test', {
            "name": {"type": "string", 'required': True, 'unique': True},
            "author": {"type": "reference", "collection": "users", "fields":["name"]},
        })

        data = {
            "name": "Bob"
        }
        ids, errs = self.db.test.insert(data)
        self.assertIsNone(errs)

        self.assertIsNone(self.db.test.find_one({'_id':2}))


    def test_allowed_string_list(self):
        self.db.register_schema('test', {
            "name": {"type": "string"},
            "tags": {"type": "list", "schema": {
                "type": "string",
                "allowed": ["top", 'middle', 'bottom'],
            }}
        })

        data = {
            "name": "Bob",
            'tags': ['lalala']
        }
        ids, errs = self.db.test.insert(data)
        self.assertIsNotNone(errs)

        self.assertEqual(errs, ["tags/0: 'lalala' not one of the allowed values"])