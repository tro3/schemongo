
Introduction
============

Schemongo is an extra layer around PyMongo to add schema enforcement to the system.  It is optimized for client-side web flows,
meaning that server-side code access is a low priority.  


Data Types
==========
    
    * Boolean
    * Integer
    * Float
    * String
    * Datetime
    * Reference
    * List of <uncontrolled datatype>
    * Hash of <uncontrolled datatype>: <uncontrolled datatype>


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
        "name": {"type": "string"},

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
        }            
    }


More terse version::

    {
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