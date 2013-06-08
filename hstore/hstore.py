import psycopg2
import psycopg2.extras
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
        self.data = {}
        self.added = {}
        self.deleted = set()
        self.sync()

    def _check_open(self):
        if self.connection.closed: raise ValueError('hstore is closed')

    def _encode(self, s):
        if not isinstance(s, basestring):
            raise TypeError('{} is not a string'.format(s))
        return s.encode('utf-8') if isinstance(s, unicode) else s

    def __getitem__(self, key):
        self._check_open()
        key = self._encode(key)
        return self.data[key]

    def __setitem__(self, key, value):
        self._check_open()
        key = self._encode(key)
        value = self._encode(value)
        self.added[key] = value
        self.data[key] = value
        if key in self.deleted:
            self.deleted.remove(key)

    def __delitem__(self, key):
        self._check_open()
        key = self._encode(key)
        self.deleted.add(key)
        del self.data[key]
        if key in self.added:
            del self.added[key]

    def __iter__(self):
        self._check_open()
        return iter(self.data)

    def __len__(self):
        self._check_open()
        return len(self.data)

    def sync(self):
        self._check_open()
        with self.connection as con, con.cursor() as c:
            # create table if needed
            c.execute("""
CREATE TABLE IF NOT EXISTS {table} (
  name text   PRIMARY KEY,
  data hstore NOT NULL DEFAULT hstore('') )
""".format(table=self.table))
            # create row if needed
            c.execute("""
INSERT INTO {table} (name) 
SELECT (%s) WHERE NOT EXISTS (SELECT name FROM {table} WHERE name = %s)
""".format(table=self.table), (self.name, self.name))
            # update hstore
            if len(self.added) > 0 or len(self.deleted) > 0:
                if len(self.deleted) > 0:
                    c.execute("""
UPDATE {table} SET data = (data || %s) - %s
WHERE name = %s
""".format(table=self.table), (self.added, list(self.deleted), self.name))
                else:
                    # pg doesn't like empty lists?
                    c.execute("""
UPDATE {table} SET data = data || %s
WHERE name = %s
""".format(table=self.table), (self.added, self.name))
            # pull hstore content
            c.execute("""
SELECT data FROM {table} 
WHERE name = %s
""".format(table=self.table), (self.name,))
            self.data = c.fetchone()[0]
            self.added.clear()
            self.deleted.clear()

    def destroy(self):
        self._check_open()
        with self.connection as con, con.cursor() as c:
            c.execute("""
DELETE FROM {table}
WHERE name = %s
""".format(table=self.table), (self.name,))

    def close(self):
        if not self.connection.closed:
            self.sync()
            self.connection.close()


        
        
        
