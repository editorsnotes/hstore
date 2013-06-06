from setuptools import setup

setup(name='hstore',
      version='0.1',
      description='Python dicts backed by PostgreSQL hstores',
      url='https://github.com/editorsnotes/hstore',
      author='Ryan Shaw',
      author_email='ryanshaw@unc.edu',
      license='Public Domain',
      packages=['hstore'],
      install_requires=['psycopg2==2.5'],
      test_suite='hstore.tests')
