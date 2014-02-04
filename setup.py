
import os

try:
    from setuptools import setup
except:
    from distutils.core import setup

from distutils.core import Command
from ke2psql.model import *
from ke2psql import config
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine, text


class InitDBCommand(Command):

    description = "Setup the database"
    user_options = []

    def initialize_options(self):
        self.cwd = None

    def finalize_options(self):
        self.cwd = os.getcwd()

    def run(self):

        url_params = dict(config.items('postgres'))
        url_params['drivername'] = 'postgresql'
        engine = create_engine(URL(**url_params))

        # Create the actual DB schema if it doesn't already exist
        # CREATE SCHEMA IF NOT EXISTS is PG 9.3
        result = engine.execute(text(u'SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = \'%s\')' % KEEMU_SCHEMA))

        if not result.scalar():
            engine.execute(text(u'CREATE SCHEMA %s AUTHORIZATION %s' % (KEEMU_SCHEMA, config.get('postgres', 'username'))))

        # Create all the tables
        Base.metadata.create_all(engine)

        print 'Created database tables'


version = '0.1'

setup(
    name='ke2psql',
    version=version,
    description='Import KE Data into postgres',
    author='Ben Scott',
    author_email='ben@benscott.co.uk',
    url='https://github.com/NaturalHistoryMuseum/ke2psql',
    license='Apache License 2.0',
    packages=[
        'ke2psql',
    ],
    cmdclass={
        'initdb': InitDBCommand
    }
)



