import os
from unittest import TestCase
import datetime

import mongomock
from schemongo import db_layer

from pprint import pprint as p


class DBLayerTests(TestCase):
    
    def setUp(self):
        self.db = db_layer.init(mongomock.MongoClient())
    

    def test_insert(self):
        self.db.collection.insert({
            "name":"bob",
            "subdoc": {
                "data": 1
            },
            "num_list": [1,2],
            "doclist": [
                {"name": "fred"},
                {"name": "george"},
            ]
        })
                
        inst = self.db.collection.find_one({})
        self.assertEqual(inst, {
            "_id": 1,
            "name":"bob",
            "subdoc": {
                "_id": 1,
                "data": 1
            },
            "num_list": [1,2],
            "doclist": [
                {"_id": 1, "name": "fred"},
                {"_id": 2, "name": "george"},
            ]
        })
        
        inst = self.db.history_find({"collection":"collection", "id":1})[0]
        self.assertEqual(inst['username'], None)
        self.assertEqual(inst['action'], 'document created')
        


    def test_find_projection(self):
        self.db.collection.insert({
            "name":"bob",
            "subdoc": {
                "data": 1,
                "data1": 2,
            },
            "num_list": [1,2],
            "doclist": [
                {"name": "fred"},
                {"name": "george"},
            ]
        })
                
        inst = self.db.collection.find({'name':'bob'}, ['subdoc'])[0]
        self.assertEqual(inst._projection, {
            "_id": 1,
            "subdoc": {
                "_id": 1,
                "data": 1,
                "data1": 2,
            },
        })

        inst = self.db.collection.find_one({'subdoc.data':1}, ['doclist'])
        self.assertEqual(inst._projection, {
            "_id": 1,
            "doclist": [
                {"_id": 1, "name": "fred"},
                {"_id": 2, "name": "george"},
            ]
        })



    def test_update(self):
        self.db.collection.insert({
            "name":"bob",
            "subdoc": {
                "data": 1,
                "data1": 2,
            },
            "num_list": [1,2],
            "doclist": [
                {"name": "fred"},
                {"name": "george"},
            ]
        })
                
        self.db.collection.update({
            "_id": 1,
            "name":"fred",
            "subdoc": {
                "_id": 1,
                "data": 2,
            },
            "num_list": [3],
            "doclist": [
                {"_id": 1},
                {"_id": 2, "name": "bob"},
                {"name": "amber"}
            ]
        }, 'admin')

        inst = self.db.collection.find_one(1)
        self.assertEqual(inst, {
            "_id": 1,
            "name":"fred",
            "subdoc": {
                "_id": 1,
                "data": 2,
                "data1": 2,
            },
            "num_list": [3],
            "doclist": [
                {"_id": 1, "name": "fred"},
                {"_id": 2, "name": "bob"},
                {"_id": 3, "name": "amber"},
            ]
        })

        inst = self.db.history_find({"collection":"collection", "id":1}, sort=[("_id",1)])[1]
        self.assertEqual(inst['username'], 'admin')
        self.assertTrue({"name": "bob"} in inst['changes'])




    def test_remove(self):
        self.db.collection.insert({
            "name":"bob",
            "subdoc": {
                "data": 1
            },
            "num_list": [1,2],
            "doclist": [
                {"name": "fred"},
                {"name": "george"},
            ]
        })
        
        self.assertEqual(self.db.collection.find().count(), 1)
        self.db.collection.remove(1, 'bob')
        self.assertEqual(self.db.collection.find().count(), 0)
        
        inst = self.db.history_find({"collection":"collection", "id":1}, sort=[("_id",1)])[1]
        self.assertEqual(inst['username'], 'bob')
        self.assertEqual(inst['action'], 'document removed')
        self.assertEqual(inst['data'], {
            "_id": 1,
            "name":"bob",
            "subdoc": {
                "_id": 1,
                "data": 1
            },
            "num_list": [1,2],
            "doclist": [
                {"_id": 1, "name": "fred"},
                {"_id": 2, "name": "george"},
            ]
        })


    def test_remove_byspec(self):
        self.db.collection.insert({
            "name":"fred",
            "subdoc": {
                "data": 1
            },
            "num_list": [1,2],
            "doclist": [
                {"name": "fred"},
                {"name": "george"},
            ]
        })        
        self.db.collection.insert({
            "name":"bob",
            "subdoc": {
                "data": 1
            },
            "num_list": [1,2],
            "doclist": [
                {"name": "fred"},
                {"name": "george"},
            ]
        })
        
        self.assertEqual(self.db.collection.find().count(), 2)
        self.db.collection.remove({"name":"bob"})
        self.assertEqual(self.db.collection.find().count(), 1)
        
        inst = self.db.history_find({"collection":"collection", "id":2}, sort=[("_id",1)])[1]
        self.assertEqual(inst['username'], None)
        self.assertEqual(inst['action'], 'document removed')
        self.assertEqual(inst['data'], {
            "_id": 2,
            "name":"bob",
            "subdoc": {
                "_id": 1,
                "data": 1
            },
            "num_list": [1,2],
            "doclist": [
                {"_id": 1, "name": "fred"},
                {"_id": 2, "name": "george"},
            ]
        })