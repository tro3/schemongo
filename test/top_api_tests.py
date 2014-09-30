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
