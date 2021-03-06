#!/usr/bin/env python

import os
from unittest import TestCase
import datetime

import mongomock
import schemongo

import json
from pprint import pprint as p


class TopAPITests(TestCase):
    
    def setUp(self):
        self.db = schemongo.init(mongomock.MongoClient())
        
        
    def test_basics(self):
        self.db.register_schema('users', {
            "username": {"type": "string"},
            "location": {"type": "string"},
        })

        self.db.register_schema('test', {
            "first_name": {"type": "string"},
            "last_name": {"type": "string", "required": True},
            "full_name": {"type": "string",
                "serialize": lambda e: "%s %s" % (e.first_name, e.last_name)
            },
            "employee_id": {"type": "integer", "unique": True},
            "subdoc": {"type": "dict", "schema": {
                "data": {"type":"integer"}
            }},
            "hash": {"type": "dict"},
            "num_list": {"type": "list", "schema": {"type": "integer"}},
            "doclist": {"type": "list", "schema": {"type": "dict", "schema": {
                "name": {"type":"string"}
            }}},            
            "user_reference": {
                'type': 'reference',
                'collection': 'users',
                'fields': ['username', 'location'],
            }    
        })

        ids, errs = self.db.users.insert({'username': 'bob', 'location': 'Paris'})
        self.assertIsNone(errs)
                
        ids, errs = self.db.test.insert({
            "first_name": "Rick",
            "last_name": "James",
            "employee_id": 284746,
            "subdoc": {
                "data": 50
            },
            "hash": {'map': True},
            "num_list": [1,2,3,4,5],
            "doclist": [
                {"name": "Geroge"}
            ],            
            "user_reference": {'_id': 1}
        })
        self.assertIsNone(errs)
        
        data = json.loads(self.db.test.find_one_and_serialize({'_id': 1}))
        self.assertEqual(data, {
            "_id": 1,
            "first_name": "Rick",
            "last_name": "James",
            "full_name": "Rick James",
            "employee_id": 284746,
            "subdoc": {
                "_id": 1,
                "data": 50
            },
            "hash": {"_id": 1, 'map': True},
            "num_list": [1,2,3,4,5],
            "doclist": [
                {"_id": 1, "name": "Geroge"}
            ],            
            "user_reference": {
                '_id': 1,
                'username': 'bob',
                'location': 'Paris'
            }
        })
        
    def test_db_api(self):
        self.db.register_schema('users', {
            "first_name": {"type": "string"},
            "last_name": {"type": "string"},
            "full_name": {"type": "string",
                "serialize": lambda e: "%s %s" % (e.first_name, e.last_name)
            }
        })

        ids, errs = self.db.users.insert({'first_name': 'Rick', 'last_name': 'James'})
        self.assertIsNone(errs)
        ids, errs = self.db.users.insert([
            {'first_name': 'Michele', 'last_name': 'Jackson'},
            {'first_name': 'Elvis', 'last_name': 'Presley'},
        ])
        self.assertIsNone(errs)
        
        self.assertEqual(self.db.users.find().count(), 3)
        self.assertIsInstance(self.db.users.find_one({'_id':2}), schemongo.DBDoc)
        self.assertEqual(self.db.users.find_one({'_id':2}).last_name, 'Jackson')
        
        errs = self.db.users.update({
            '_id': 2,
            'first_name': 'Michael'
        })
        self.assertIsNone(errs)

        inst = self.db.users.find_one({'last_name':'Jackson'})
        self.assertEqual(json.loads(self.db.users.serialize(inst)), {
            '_id': 2,
            'first_name': 'Michael',
            'last_name': 'Jackson',
            'full_name': 'Michael Jackson'
        })
        
        self.assertEqual(json.loads(self.db.users.find_and_serialize({}, fields=['full_name', 'last_name'], sort=[('last_name', -1)])), [
            {'_id':3, 'full_name': 'Elvis Presley', 'last_name': 'Presley'},
            {'_id':1, 'full_name': 'Rick James', 'last_name': 'James'},
            {'_id':2, 'full_name': 'Michael Jackson', 'last_name': 'Jackson'},
        ])
                
        self.db.users.remove({'_id':2})        
        self.assertEqual(json.loads(self.db.users.find_and_serialize({}, fields=['full_name', 'last_name'], sort=[('last_name', -1)])), [
            {'_id':3, 'full_name': 'Elvis Presley', 'last_name': 'Presley'},
            {'_id':1, 'full_name': 'Rick James', 'last_name': 'James'},
        ])


    def test_db_serial_dict(self):
        self.db.register_schema('users', {
            "first_name": {"type": "string"},
            "last_name": {"type": "string"},
            "full_name": {"type": "string",
                "serialize": lambda e: "%s %s" % (e.first_name, e.last_name)
            }
        })

        ids, errs = self.db.users.insert([
            {'first_name': 'Rick', 'last_name': 'James'},
            {'first_name': 'Michael', 'last_name': 'Jackson'},
            {'first_name': 'Elvis', 'last_name': 'Presley'},
        ])
        self.assertIsNone(errs)

        inst = self.db.users.find_one_and_serial_dict({'last_name':'Jackson'})
        self.assertEqual(inst, {
            '_id': 2,
            'first_name': 'Michael',
            'last_name': 'Jackson',
            'full_name': 'Michael Jackson'
        })
        
        self.assertEqual(self.db.users.find_and_serial_dict({}, fields=['full_name', 'last_name'], sort=[('last_name', -1)]), [
            {'_id':3, 'full_name': 'Elvis Presley', 'last_name': 'Presley'},
            {'_id':1, 'full_name': 'Rick James', 'last_name': 'James'},
            {'_id':2, 'full_name': 'Michael Jackson', 'last_name': 'Jackson'},
        ])


    def test_schema_change(self):
        self.db.register_schema('users', {
            "first_name": {"type": "string", 'default': 'bob'},
        })

        ids, errs = self.db.users.insert([
            {'last_name2': 'James'},
        ], direct=True)
        self.assertIsNone(errs)

        errs = self.db.users.update({'_id':1})
        self.assertIsNone(errs)

        inst = self.db.users.find_one_and_serial_dict(1)
        self.assertEqual(inst, {
            '_id': 1,
            'first_name': 'bob',
        })
        
        
    def test_find_nonexistent(self):
        self.db.register_schema('users', {
            "first_name": {"type": "string"},
            "last_name": {"type": "string"},
            "full_name": {"type": "string",
                "serialize": lambda e: "%s %s" % (e.first_name, e.last_name)
            }
        })

        ids, errs = self.db.users.insert([
            {'first_name': 'Rick', 'last_name': 'James'},
            {'first_name': 'Michael', 'last_name': 'Jackson'},
            {'first_name': 'Elvis', 'last_name': 'Presley'},
        ])
        self.assertIsNone(errs)

        inst = self.db.users.find_one({'last_name':'Jackson2'})
        self.assertIsNone(inst)


    def test_nonaligned_serialization(self):
        self.db.register_schema('users', {
            "first_name": {"type": "string"},
            "last_name": {"type": "string"},
            "contacts": {"type": "list", "schema": {"type": "dict", "schema": {
                "name": {"type": "string"},
                "ident": {"type": "string",
                    "serialize": lambda e: "%s/%s" % (e.get_root().last_name, e.name)
                }
            }}}
        })

        ids, errs = self.db.users.insert({
            'first_name': 'Rick',
            'last_name': 'James',
            'contacts': [
                {'name': 'Jake Johanson'},
                {'name': 'Bob Bobberton'},
                {'name': 'Fred Farthington'},                
            ]
        })
        self.assertIsNone(errs)

        inst = self.db.users.find_one({'last_name':'James'}, {'contacts': 1})
        inst._projection['contacts'].pop(1)
        data = self.db.users.serialize(inst)
        data = json.loads(data)
        self.assertEqual(data, {
            '_id': 1,
            'contacts': [
                {'_id': 1, 'name': 'Jake Johanson', 'ident': 'James/Jake Johanson'},
                {'_id': 3, 'name': 'Fred Farthington', 'ident': 'James/Fred Farthington'},                                
            ]
        })
        


    def test_schema_change_add_ref(self):
        self.db.register_schema('users', {
            "name": {"type": "string", 'required': True, 'unique': True},
        })        

        self.db.register_schema('test', {
            "first_name": {"type": "string", 'default': 'bob'},
        })

        ids, errs = self.db.test.insert([
            {'first_name': 'James'},
        ], direct=True)
        self.assertIsNone(errs)

        self.db.register_schema('test', {
            "first_name": {"type": "string", 'default': 'bob'},
            "users": {
                "type": "reference",
                "collection": "users",
                "fields": [
                    "name"
                ]
            }
        })

        errs = self.db.test.update({'_id':1})
        self.assertIsNone(errs)

        data = json.loads(self.db.test.serialize(self.db.test.find_one({'_id':1})))
        self.assertEqual(data, {
            '_id': 1,
            'first_name': 'James',
            'users': None,
        })


    def test_schema_change_add_obj(self):
        self.db.register_schema('users', {
            "name": {"type": "string", 'required': True, 'unique': True},
        })        

        self.db.register_schema('test', {
            "first_name": {"type": "string"},
        })

        ids, errs = self.db.test.insert([
            {'first_name': 'James'},
        ], direct=True)
        self.assertIsNone(errs)

        self.db.register_schema('test', {
            "first_name": {"type": "string"},
            "user": {"type": 'dict', 'schema': {
                'username': {"type": "string"}
            }}
        })

        errs = self.db.test.update({'_id':1})
        self.assertIsNone(errs)

        data = json.loads(self.db.test.serialize(self.db.test.find_one({'_id':1})))
        self.assertEqual(data, {
            '_id': 1,
            'first_name': 'James',
            'user': {
                '_id': 1,
                'username': None
            },
        })



    def test_schema_change_add_reflist(self):
        self.db.register_schema('users', {
            "name": {"type": "string", 'required': True, 'unique': True},
        })        

        self.db.register_schema('test', {
            "first_name": {"type": "string", 'default': 'bob'},
        })

        ids, errs = self.db.test.insert([
            {'first_name': 'James'},
        ], direct=True)
        self.assertIsNone(errs)

        self.db.register_schema('test', {
            "first_name": {"type": "string", 'default': 'bob'},
            "users": {"type": "list", "schema": {
                "type": "reference",
                "collection": "users",
                "fields": [
                    "name"
                ]
            }}
        })

        errs = self.db.test.update({'_id':1})
        self.assertIsNone(errs)

        data = json.loads(self.db.test.serialize(self.db.test.find_one({'_id':1})))
        self.assertEqual(data, {
            '_id': 1,
            'first_name': 'James',
            'users': [],
        })


    def test_schema_change_add_objlist(self):
        self.db.register_schema('users', {
            "name": {"type": "string", 'required': True, 'unique': True},
        })        

        self.db.register_schema('test', {
            "first_name": {"type": "string", 'default': 'bob'},
        })

        ids, errs = self.db.test.insert([
            {'first_name': 'James'},
        ], direct=True)
        self.assertIsNone(errs)

        self.db.register_schema('test', {
            "first_name": {"type": "string", 'default': 'bob'},
            "users": {"type": "list", "schema": {'type': 'dict', 'schema': {
                'username': {"type": "string"}
            }}}
        })

        errs = self.db.test.update({'_id':1})
        self.assertIsNone(errs)

        data = json.loads(self.db.test.serialize(self.db.test.find_one({'_id':1})))
        self.assertEqual(data, {
            '_id': 1,
            'first_name': 'James',
            'users': [],
        })



    def test_schema_change_major(self):
        self.db.register_schema('users', {
            "name": {"type": "string", 'required': True, 'unique': True},
        })        

        ids, errs = self.db.users.insert({'name': 'James'})
        self.assertIsNone(errs)


        self.db.register_schema('test', {
            "name": {"type": "string"},
            "data": {"type": "integer", "required": True},
            "cap_name": {"type": "string",
                "serialize": lambda e: e.name.upper()
            },
            "subdoc": {"type": "dict", "schema": {
                "data": {"type":"integer"},
                "subdoc": {"type": "dict", "schema": {
                    "data": {"type":"integer"},
                }},
                "doclist": {"type": "list", "schema": {"type": "dict", "schema": {
                    "name": {"type":"string"}
                }}},            
                "ref": {
                    'type': 'reference',
                    'collection': 'users',
                    'fields': ['name'],
                }, 
                "reflist": {"type": "list", "schema": {
                    'type': 'reference',
                    'collection': 'users',
                    'fields': ['name'],
                }},            
            }},
            "list": {"type": "list"},
            "hash": {"type": "dict"},
            "num_list": {"type": "list", "schema": {"type": "integer"}},
            "doclist": {"type": "list", "schema": {"type": "dict", "schema": {
                "data": {"type":"integer"},
                "subdoc": {"type": "dict", "schema": {
                    "data": {"type":"integer"},
                }},
                "doclist": {"type": "list", "schema": {"type": "dict", "schema": {
                    "name": {"type":"string"}
                }}},            
                "ref": {
                    'type': 'reference',
                    'collection': 'users',
                    'fields': ['name'],
                }, 
                "reflist": {"type": "list", "schema": {
                    'type': 'reference',
                    'collection': 'users',
                    'fields': ['name'],
                }},            
            }}},
            "ref": {
                'type': 'reference',
                'collection': 'users',
                'fields': ['name'],
            }, 
            "reflist": {"type": "list", "schema": {
                'type': 'reference',
                'collection': 'users',
                'fields': ['name'],
            }},            
        })

        data = {
            "name": 'fred',
            "data": 1,
            "subdoc": {
                "data": 1,
                "subdoc": {"data": 56},
                "doclist": [{"name": 'bob'}],            
                "ref": {'_id':1},
                "reflist": [{'_id':1}],    
            },
            "list": [4,'fgh'],
            "hash": {3:'bob', '4':45},
            "num_list": [5,4,3],
            "doclist": [{
                "data": 1,
                "subdoc": {"data": 56},
                "doclist": [{"name": 'bob'}],            
                "ref": {'_id':1},
                "reflist": [{'_id':1}],    
            }],
            "ref": {'_id':1},
            "reflist": [{'_id':1}],    
        }

        ids, errs = self.db.test.insert(data)
        self.assertIsNone(errs)


        self.db.register_schema('test', {
            "name2": {"type": "string"},
            "data2": {"type": "integer", "required": True, 'default':1},
            "cap_name2": {"type": "string",
                "serialize": lambda e: e.name2 and e.name2.upper()
            },
            "subdoc2": {"type": "dict", "schema": {
                "data": {"type":"integer"},
                "subdoc": {"type": "dict", "schema": {
                    "data": {"type":"integer"},
                }},
                "doclist": {"type": "list", "schema": {"type": "dict", "schema": {
                    "name": {"type":"string"}
                }}},            
                "ref": {
                    'type': 'reference',
                    'collection': 'users',
                    'fields': ['name'],
                }, 
                "reflist": {"type": "list", "schema": {
                    'type': 'reference',
                    'collection': 'users',
                    'fields': ['name'],
                }},            
            }},
            "list2": {"type": "list"},
            "hash2": {"type": "dict"},
            "num_list2": {"type": "list", "schema": {"type": "integer"}},
            "doclist2": {"type": "list", "schema": {"type": "dict", "schema": {
                "data": {"type":"integer"},
                "subdoc": {"type": "dict", "schema": {
                    "data": {"type":"integer"},
                }},
                "doclist": {"type": "list", "schema": {"type": "dict", "schema": {
                    "name": {"type":"string"}
                }}},            
                "ref": {
                    'type': 'reference',
                    'collection': 'users',
                    'fields': ['name'],
                }, 
                "reflist": {"type": "list", "schema": {
                    'type': 'reference',
                    'collection': 'users',
                    'fields': ['name'],
                }},            
            }}},
            "ref2": {
                'type': 'reference',
                'collection': 'users',
                'fields': ['name'],
            }, 
            "reflist2": {"type": "list", "schema": {
                'type': 'reference',
                'collection': 'users',
                'fields': ['name'],
            }},            
        })

        errs = self.db.test.update({'_id':1})
        self.assertIsNone(errs)
        resp = self.db.test.find_one_and_serialize(1)
        resp = json.loads(resp)
        self.assertEqual(resp, {
            "_id": 1,
            "name2": None,
            "cap_name2": None,
            "data2": 1,
            "subdoc2": {
                "_id": 1,
                "data": None,
                "subdoc": {"_id": 1, "data": None},
                "doclist": [],            
                "ref": None,
                "reflist": [],    
            },
            "list2": [],
            "hash2": {"_id":1},
            "num_list2": [],
            "doclist2": [],
            "ref2": None,
            "reflist2": [],    
        })