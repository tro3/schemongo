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

        errs = self.db.users.insert({'username': 'bob', 'location': 'Paris'})
        self.assertIsNone(errs)
                
        errs = self.db.test.insert({
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