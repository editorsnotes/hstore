======
hstore
======

hstore provides objects that act just like ordinary dicts except they
are backed by Postgresql hstores.

Quick start
-----------

Use like this::

    import hstore
    d = hstore.open('postgresql://user@localhost/hstore_test', 'mydata')
    d['foo'] = 'bar'
    print d['foo']
    del d['foo']

Keys and values are UTF-8 strings. As a convenience, you may use
Unicode keys and values, but these are converted and stored as UTF-8
strings and values will be returned as such::

    d['unicode'] = u'日本語'
    assert isinstance(d['unicode'], str)
    assert d['unicode'] == u'日本語'.encode('utf-8')



