import psycopg2
import psycopg2.extras
import psycopg2.extensions
from collections import MutableMapping

def _execute(c, func, close=False):
    if hasattr(c, 'cursor'):
        close = False
    else:
        c = psycopg2.connect(c)
    try:
        return func(c)
    finally:
        if close: c.close()

def open(c, name, table='hstores'):
    def open_hstore(c):
        c.cursor().execute('CREATE EXTENSION IF NOT EXISTS hstore')
        psycopg2.extras.register_hstore(c)
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, c)
        return Hstore(c, name, table)
    return _execute(c, open_hstore)

def exists(c, name, table='hstores'):
    def hstore_exists(c):
        with c.cursor() as cur:
            try:
                cur.execute("""
SELECT COUNT(name) FROM {table}
WHERE name = %s
""".format(table=table), (name,))
                return (cur.fetchone()[0] > 0)
            except psycopg2.ProgrammingError as e:
                if e.pgcode == '42P01':
                    return False
                raise e
    return _execute(c, hstore_exists, close=True)

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

    def itervalues(self):
        with self._get_connection().cursor() as c: 
            c.execute("""
SELECT svals(data) FROM {table}
WHERE name = %s
""".format(table=self.table), (self.name,))
            for r in c:
                yield r[0]

    def iteritems(self):
        with self._get_connection().cursor() as c: 
            c.execute("""
SELECT hstore_to_matrix(data) FROM {table}
WHERE name = %s
""".format(table=self.table), (self.name,))
            for pair in c.fetchone()[0]:
                yield tuple(pair)

    def items(self):
        return list(self.iteritems())

    def values(self):
        return list(self.itervalues())

    def __len__(self):
        with self._get_connection().cursor() as c: 
            c.execute("""
SELECT array_length(akeys(data), 1) FROM {table}
WHERE name = %s
""".format(table=self.table), (self.name,))
            return c.fetchone()[0] or 0

    def close(self):
        self.connection.close()

    def __del__(self):
        if not self.connection.closed:
            with self.connection as con, con.cursor() as c:
                c.execute("""
DELETE FROM {table}
WHERE name = %s
""".format(table=self.table), (self.name,))

        
        
        
