import psycopg2
import psycopg2.extras
import psycopg2.extensions
from collections import MutableMapping

def open(c, name, table='hstores'):
    if not hasattr(c, 'cursor'):
        c = psycopg2.connect(c)   
    c.cursor().execute('CREATE EXTENSION IF NOT EXISTS hstore')
    psycopg2.extras.register_hstore(c)
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, c)
    return Hstore(c, name, table)

class Hstore(MutableMapping):

    def __init__(self, connection, name, table):
        self.connection = connection
        self.name = name
        self.table = table
        with self.connection as con, con.cursor() as c:
            c.execute("""
CREATE TABLE IF NOT EXISTS {table} (
  name text   PRIMARY KEY,
  data hstore NOT NULL DEFAULT hstore('') )
""".format(table=self.table))
            c.execute("""
INSERT INTO {table} (name) 
SELECT (%s) WHERE NOT EXISTS (SELECT name FROM {table} WHERE name = %s)
""".format(table=self.table), (self.name, self.name))

    def _get_connection(self):
        if self.connection.closed:
            raise ValueError('hstore is closed')
        return self.connection 

    def __getitem__(self, key):
        if not isinstance(key, basestring):
            raise TypeError('keys must be strings')
        with self._get_connection().cursor() as c: 
            c.execute("""
SELECT data -> %s FROM {table} 
WHERE name = %s
""".format(table=self.table), (key, self.name))
            value = c.fetchone()[0]
            if value is None:
                raise KeyError(key)
            return value

    def __setitem__(self, key, value):
        if not isinstance(key, basestring):
            raise TypeError('keys must be strings')
        if not isinstance(value, basestring):
            raise TypeError('values must be strings')
        with self._get_connection() as con, con.cursor() as c:
            c.execute("""
UPDATE {table} SET data = data || hstore(%s, %s) 
WHERE name = %s
""".format(table=self.table), (key, value, self.name))

    def __delitem__(self, key):
        if not isinstance(key, basestring):
            raise TypeError('keys must be strings')
        with self._get_connection() as con, con.cursor() as c:
            c.execute("""
UPDATE {table} SET data = delete(data, %s) 
WHERE name = %s
""".format(table=self.table), (key, self.name))

    def __iter__(self):
        with self._get_connection().cursor() as c: 
            c.execute("""
SELECT skeys(data) FROM {table}
WHERE name = %s
""".format(table=self.table), (self.name,))
            for r in c:
                yield r[0]

    def __len__(self):
        with self._get_connection().cursor() as c: 
            c.execute("""
SELECT array_length(akeys(data), 1) FROM {table}
WHERE name = %s
""".format(table=self.table), (self.name,))
            return c.fetchone()[0] or 0

    def close(self):
        self.connection.close()
        
