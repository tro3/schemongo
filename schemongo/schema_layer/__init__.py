
from database import SchemaDatabaseWrapper
#from schema_doc import SchemaDoc

db = None

def init(client=None, dbname=None):
    global db
    db = SchemaDatabaseWrapper(client, dbname)
    return db