
from database import SchemaDatabaseWrapper


db = None

def init(client=None, dbname=None):
    global db
    db = SchemaDatabaseWrapper(client, dbname)
    return db