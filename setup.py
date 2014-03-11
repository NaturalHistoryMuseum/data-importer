
import os

try:
    from setuptools import setup
except:
    from distutils.core import setup

from distutils.core import Command
from ConfigParser import ConfigParser
from ConfigParser import NoSectionError
try:
    from ke2sql.model import meta
    from ke2sql.model.base import Base
    # Required to create the tables
    from ke2sql.model.keemu import *
    from ke2sql.model.log import Log
    from sqlalchemy import text
except NoSectionError:
    pass

class InitDBCommand(Command):

    description = "Setup the database"
    user_options = []

    def initialize_options(self):
        self.cwd = None

    def finalize_options(self):
        self.cwd = os.getcwd()

    def run(self):

        config = ConfigParser()
        config.read(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'ke2sql/client.cfg'))

        # Create the actual DB schema if it doesn't already exist
        # CREATE SCHEMA IF NOT EXISTS is PG 9.3
        result = meta.engine.execute(text(u'SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = \'%s\')' % config.get('database', 'schema')))

        if not result.scalar():
            meta.engine.execute(text(u'CREATE SCHEMA %s AUTHORIZATION %s' % (config.get('database', 'schema'), config.get('database', 'username'))))

        # Create all the tables
        Base.metadata.create_all(meta.engine)

        print 'Creating database tables: SUCCESS'


version = '0.1'

setup(
    name='ke2sql',
    version=version,
    description='Import KE Data into postgres',
    author='Ben Scott',
    author_email='ben@benscott.co.uk',
    url='https://github.com/NaturalHistoryMuseum/ke2sql',
    license='Apache License 2.0',
    packages=[
        'ke2sql',
    ],
    cmdclass={
        'initdb': InitDBCommand
    }
)



