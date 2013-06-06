# -*- coding: utf-8 -*-

import os
import hstore
import psycopg2
from psycopg2.extensions import \
    ISOLATION_LEVEL_AUTOCOMMIT, ISOLATION_LEVEL_READ_COMMITTED
from unittest import TestCase


connection_uri = os.environ.get(
    'DBURI', 'postgresql://unittest@localhost/hstore_test')

class HstoreTestCase(TestCase):

    def execute(self, command):
        if command in [ 'CREATE', 'DROP' ]:
            server, dbname = connection_uri.rsplit('/',1) 
            c = psycopg2.connect('{}/template1'.format(server))
            try:
                c.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                c.cursor().execute('{} DATABASE {}'.format(command, dbname))
                c.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)
            finally:
                c.close()
    
    def setUp(self):
        self.execute('CREATE')
        self.hstores = []
        self.connections = []

    def open_hstore(self, connection_or_uri, name, table='hstores'):
        d = hstore.open(connection_or_uri, name, table=table)
        self.hstores.append(d)
        return d

    def open_connection(self, uri):
        c = psycopg2.connect(uri)
        self.connections.append(c)
        return c

    def test_open(self):
        d = self.open_hstore(connection_uri, 'test')

    def test_exists(self):
        self.assertFalse(
            hstore.exists(connection_uri, 'test'))
        self.assertFalse(
            hstore.exists(self.open_connection(connection_uri), 'test'))
        self.open_hstore(connection_uri, 'test')
        self.assertTrue(
            hstore.exists(connection_uri, 'test'))
        self.assertTrue(
            hstore.exists(self.open_connection(connection_uri), 'test'))

    def test_open_two_with_same_name(self):
        # same underlying table
        d1 = self.open_hstore(connection_uri, 'test')
        d2 = self.open_hstore(connection_uri, 'test')
        d1['a'] = 'b'
        self.assertEqual(d2['a'], 'b')
        d2['a'] = 'c'
        self.assertEqual(d1['a'], 'c')
        
    def test_open_two_with_different_tablenames(self):
        # different underlying table
        d1 = self.open_hstore(connection_uri, 'test')
        d2 = self.open_hstore(connection_uri, 'test', table='anothertable')
        d1['a'] = 'b'
        with self.assertRaises(KeyError):
            d2['a']
        d2['a'] = 'c'
        self.assertEqual(d1['a'], 'b')
        
    def test_open_with_existing_connection(self):
        c = self.open_connection(connection_uri)
        d = self.open_hstore(c, 'test')
        d.close()
        self.assertTrue(c.closed)
        
    def test_open_and_close(self):
        d = self.open_hstore(connection_uri, 'test')
        d.close()
        
    def test_open_and_close_twice(self):
        d = self.open_hstore(connection_uri, 'test')
        d.close()
        d.close()

    def test_reopen(self):
        d = self.open_hstore(connection_uri, 'test')
        d.close()
        d = self.open_hstore(connection_uri, 'test')

    def test_store_value(self):
        d = self.open_hstore(connection_uri, 'test')
        d['foo'] = 'bar'
        
    def test_access_after_close(self):
        d = self.open_hstore(connection_uri, 'test')
        d.close()
        with self.assertRaises(ValueError):
            d['foo']
        with self.assertRaises(ValueError):
            d['foo'] = 'bar'
        with self.assertRaises(ValueError):
            len(d)
        with self.assertRaises(ValueError):
            list(d.iteritems())
        
    def test_store_and_retrieve_value(self):
        d = self.open_hstore(connection_uri, 'test')
        d['foo'] = 'bar'
        d.close()
        d = self.open_hstore(connection_uri, 'test')
        self.assertEqual(d['foo'], 'bar')
        self.assertTrue(isinstance(d['foo'], str))

    def test_store_and_retrieve_unicode(self):
        d = self.open_hstore(connection_uri, 'test')
        d['foo'] = u'日本語'
        d.close()
        d = self.open_hstore(connection_uri, 'test')
        # comes back as a UTF-8 string
        self.assertEqual(d['foo'], u'日本語'.encode('utf-8'))
        self.assertTrue(isinstance(d['foo'], str))

    def test_unicode_key(self):
        d = self.open_hstore(connection_uri, 'test')
        d[u'日本語'] = 'foo'
        d.close()
        d = self.open_hstore(connection_uri, 'test')
        self.assertEqual(d[u'日本語'], 'foo')
        
    def test_unicode_storename(self):
        d = self.open_hstore(connection_uri, u'日本語')
        d['bar'] = 'foo'
        d.close()
        d = self.open_hstore(connection_uri, u'日本語')
        self.assertEqual(d['bar'], 'foo')

    def test_store_nonstring(self):
        d = self.open_hstore(connection_uri, 'test')
        with self.assertRaises(TypeError):
            d['foo'] = 7

    def test_nonstring_key(self):
        d = self.open_hstore(connection_uri, 'test')
        with self.assertRaises(TypeError):
            d[7] = 'foo'

    def test_get_nonexistent_key(self):
        d = self.open_hstore(connection_uri, 'test')
        with self.assertRaises(KeyError):
            d['notthere']

    def test_delete_key(self):
        d = self.open_hstore(connection_uri, 'test')
        d['foo'] = 'bar'
        self.assertEqual(d['foo'], 'bar')
        del d['foo']
        with self.assertRaises(KeyError):
            d['foo']

    def test_length(self):
        d = self.open_hstore(connection_uri, 'test')
        self.assertEqual(len(d), 0)
        d['foo'] = 'bar'
        self.assertEqual(len(d), 1)
        d['foo'] = 'baz'
        self.assertEqual(len(d), 1)
        d['bar'] = 'baz'
        self.assertEqual(len(d), 2)

    def test_iterate(self):
        d = self.open_hstore(connection_uri, 'test')
        d['foo'] = 'bar'
        d['bar'] = 'baz'
        self.assertEqual(sorted(d.iteritems()), 
                         [(u'bar',u'baz'),(u'foo',u'bar')])

    def test_itervalues(self):
        d = self.open_hstore(connection_uri, 'test')
        d['foo'] = 'bar'
        d['bar'] = 'baz'
        d['xxx'] = 'bar'
        self.assertEqual(sorted(d.itervalues()), 
                         [u'bar',u'bar',u'baz'])

    def test_items(self):
        d = self.open_hstore(connection_uri, 'test')
        d['foo'] = 'bar'
        d['bar'] = 'baz'
        self.assertEqual(sorted(d.items()), 
                         [(u'bar',u'baz'),(u'foo',u'bar')])

    def test_values(self):
        d = self.open_hstore(connection_uri, 'test')
        d['foo'] = 'bar'
        d['bar'] = 'baz'
        self.assertEqual(sorted(d.values()), 
                         [u'bar',u'baz'])

    def test_contains(self):
        d = self.open_hstore(connection_uri, 'test')
        self.assertFalse('foo' in d)
        d['foo'] = 'bar'
        self.assertTrue('foo' in d)
        del d['foo']
        self.assertFalse('foo' in d)

    def test_clear(self):
        d = self.open_hstore(connection_uri, 'test')
        d['foo'] = 'bar'
        d['bar'] = 'baz'
        self.assertEqual(len(d), 2)
        d.clear()
        self.assertEqual(len(d), 0)

    def test_update(self):
        d = self.open_hstore(connection_uri, 'test')
        d.update({'foo':'bar','bar':'baz'})
        self.assertEqual(len(d), 2)
        self.assertEqual(d['foo'], 'bar')
        self.assertEqual(d['bar'], 'baz')

    def test_equality(self):
        d = self.open_hstore(connection_uri, 'test')
        d.update({'foo':'bar','bar':'baz'})
        self.assertEqual(d, {'foo':'bar','bar':'baz'})

    def test_destroy(self):
        self.assertFalse(hstore.exists(connection_uri, 'test'))
        c = self.open_connection(connection_uri)
        d = hstore.open(c, 'test')
        self.assertTrue(hstore.exists(connection_uri, 'test'))
        del d
        self.assertFalse(hstore.exists(connection_uri, 'test'))

    def test_destroy_after_close(self):
        self.assertFalse(hstore.exists(connection_uri, 'test'))
        c = self.open_connection(connection_uri)
        d = hstore.open(c, 'test')
        self.assertTrue(hstore.exists(connection_uri, 'test'))
        d.close()
        del d
        # still exists because we closed it first
        self.assertTrue(hstore.exists(connection_uri, 'test'))

    def tearDown(self):
        [ h.close() for h in self.hstores ]
        for c in self.connections: del c
        self.hstores = []
        self.connections = []
        self.execute('DROP')
        
