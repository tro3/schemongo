
py-schemongo
============

Schemongo is an extra layer around PyMongo to add schema enforcement to the system.  It is optimized for client-side web flows,
meaning that server-side code access is a low priority.  


Data Types
==========
    
    * Boolean *(type: 'boolean')*
    * Integer *(type: 'integer')*
    * Float *(type: 'float')*
    * String *(type: 'string')*
    * Datetime *(type: 'datetime')*
    * Reference *(type: 'reference')*
    * List of <datatype> *(type: 'list', with schema)*
    * Object *(type: 'dict', with schema)*
    * List of <uncontrolled datatype> *(type: 'list', no schema)*
    * Hash of <uncontrolled datatype>: <uncontrolled datatype> *(type: 'dict', no schema)*


Behaviors
=========

    * default
    * auto
    * auto_init
    * serialize
    * read_only
    * allowed
    * required
    * unique



Schema Format
=============

Normal version::

    {
        "first_name": {"type": "string"},
        
        "last_name": {
            "type": "string",
            "required": True
        },
        
        "full_name": {
            "type": "string",
            "serialize": lambda element: "%s %s" % (element.first_name, element.last_name)
        },

        "employee_id": {
            "type": "integer",
            "unique": True
        },

        "subdoc": {
            "type": "dict",
            "schema": {
                "data": {"type":"integer"}
            }
        },

        "hash": {"type": "dict"},

        "num_list": {
            "type": "list",
            "schema": {"type": "integer"}
        },

        "doclist": {
            "type": "list",
            "schema": {
                "type": "dict",
                "schema": {
                    "name": {"type":"string"}
                }
            }
        },
        
        "user_reference": {
            'type': 'reference',
            'collection': 'users',
            'fields': ['username', 'location'],
        }

    }


More terse version::

    {
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

    }



API
===

Module Level
------------

schemongo.init()


Schemongo DB
------------

db.insert()
db.update()
db.remove()
db.find()
db.find_one()
db.serialize()
db.find_and_serialize()
db.find_one_and_serialize()


Schemongo DBDoc
--------------

dbdoc.get_parent()
dbdoc.get_root()


To Do
-----

Improve response interface
Reference error handling
