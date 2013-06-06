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


