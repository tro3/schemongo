
from database import DatabaseWrapper

db = None

def init(client=None, dbname=None):
    global db
    db = DatabaseWrapper(client, dbname)
    return db