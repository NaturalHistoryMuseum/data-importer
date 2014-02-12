
import os

try:
    from setuptools import setup
except:
    from distutils.core import setup

from distutils.core import Command
from ConfigParser import ConfigParser
from ke2psql.model.keemu import *
from ke2psql.model import meta
from sqlalchemy import text

class InitDBCommand(Command):

    description = "Setup the database"
    user_options = []

    def initialize_options(self):
        self.cwd = None

    def finalize_options(self):
        self.cwd = os.getcwd()

    def run(self):

        config = ConfigParser()
        config.read(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'ke2psql/client.cfg'))

        # Create the actual DB schema if it doesn't already exist
        # CREATE SCHEMA IF NOT EXISTS is PG 9.3
        result = meta.engine.execute(text(u'SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = \'%s\')' % KEEMU_SCHEMA))

        if not result.scalar():
            meta.engine.execute(text(u'CREATE SCHEMA %s AUTHORIZATION %s' % (KEEMU_SCHEMA, config.get('database', 'username'))))

        # Create all the tables
        Base.metadata.create_all(meta.engine)

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



