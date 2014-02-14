#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import sys
import os
import unittest
import luigi
from ke2psql.model import meta
from ke2psql.model.meta import config
from ke2psql.tasks.ke import KEFileTask
import abc
from keparser import KEParser
from datetime import date, datetime
from sqlalchemy.orm import class_mapper
from sqlalchemy.orm.properties import RelationshipProperty as SQLAlchemyRelationshipProperty
from ke2psql.model.keemu import RelationshipProperty
from ke2psql.model.keemu import *

class BaseTask(object):

    export_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    file_name = luigi.Parameter()

    @abc.abstractproperty
    def module(self):
        return None

    def requires(self):
        return [KEFileTask(module=self.module, export_dir=self.export_dir, compressed=False, file_name=self.file_name)]

    def finish(self):
        # Do not mark complete
        pass


class BaseTest(object):

    session = meta.session
    irn = 1
    file_name = 'export'

    @abc.abstractproperty
    def task(self):
        return None

    @abc.abstractproperty
    def model(self):
        return None

    def test_import(self):
        """
        Import the data and check it's there
        """
        self.create()
        obj = self.query().one()
        self.delete()

    def test_data(self):
        """
        Ensure the data in the file matches what is in the DB
        """
        self.create()
        data = self.get_data().next()

        # Load the obj from the database
        obj = self.query().one()

        for prop in self.model().__mapper__.iterate_properties:

            if type(prop) not in [RelationshipProperty, SQLAlchemyRelationshipProperty]:

                obj_value = getattr(obj, prop.key)
                # In these tests, we should have a value for every field

                # self.assertIsNotNone(obj_value, 'Field %s is none' % prop.key)

                # Skip type, _created and _inserted dates
                # As long as they're not null we're good
                if prop.key.startswith('_') or prop.key in ['type']:
                    continue

                if isinstance(obj_value, date):
                    obj_value = unicode(obj_value)

                col = prop.columns[0]
                field = col.alias or prop.key
                value = data.get(field)
                self.assertEqual(obj_value, value, '%s does not match %s %s' % (prop.key, obj_value, value))

        # Destroy the object
        self.delete()

    def test_delete(self):
        """
        Delete the record and ensure it's no longer available
        """
        # Ensure the data is there to be deleted
        self.create()
        # ANd then delete it
        self.delete()
        count = self.query().count()
        self.assertEqual(count, 0)

    def test_type(self):
        self.create()
        obj = self.query().one()
        polymorphic_type = self.model.__mapper_args__['polymorphic_identity']
        self.assertEqual(polymorphic_type, obj.type)
        self.delete()

    def test_update(self):
        """
        Import the data and check it's there
        """
        self.create()
        # Rerun with the update import
        self.file_name = 'update'
        self.create()

    def test_update(self):
        self.update()
        self.create()

    def test_update_data(self):
        self.update()
        self.test_data()

    def update(self):
        """
        Helper function for running updates
        Just run once and then rename the import file
        """
        self.create()
        self.file_name = self.file_name.replace('export', 'update')

    def create(self):
        luigi.build([self.task(file_name=self.file_name)], local_scheduler=True)

    def delete(self):
        obj = self.query().one()
        # Delete the obj
        self.session.delete(obj)
        self.session.commit()

    def query(self):
        return self.session.query(self.model).filter(self.model.irn == self.irn)

    def get_data(self):
        keemu_schema_file = config.get('keemu', 'schema')

        #  Find the path to the import file from the import task
        task = self.task(file_name=self.file_name)

        # TODO: This will fail on catalogue
        file_task = task.requires()[0]
        file_path = file_task.get_file_path()
        return KEParser(open(file_path, 'r'), schema_file=keemu_schema_file, input_file_path=file_path)

    def _test_relationship_exists(self, relationship):
        """
        Check a relationship exists
        """
        self.create()
        obj = self.query().one()
        value = getattr(obj, relationship)

        # Check we have the object
        self.assertTrue(len(value) == 1, 'Missing relationship %s' % relationship)

        # Retrieve the model to test against
        model = getattr(self.model, relationship).mapper.class_
        # Check the object is the right type
        self.assertIsInstance(value[0], model)

    def _test_relationship_values(self, relationship, count=1):
        """
        Loop through all values in a given relationship
        Checking the data from the KE export matches whats in the DB
        """
        self.create()
        obj = self.query().one()
        data = self.get_data().next()

        x = 0
        attr = getattr(obj, relationship)

        # Ensure we have the right number of values
        self.assertEqual(len(attr), count, 'There should be %s nodes in relationship %s (%s found)' % (count, relationship, len(attr)))

        for rel_obj in getattr(obj, relationship):

            aliases = rel_obj.get_aliases()

            for alias, db_field in aliases.items():

                obj_value = getattr(rel_obj, db_field)

                value = data.get(alias, None)

                if value:
                    value = self.ensure_list(value)

                    if x in value:
                        self.assertEqual(obj_value, value[x], '%s does not match %s %s' % (db_field, obj_value, value[x]))

        self.delete()

    def _test_relationship_delete(self, relationship):
        """
        For a given relationship, check all records in the associated tables have been deleted
        """
        self.create()
        self.delete()
        table = getattr(self.model, relationship).property.table
        count = self.session.scalar("SELECT COUNT(*) FROM {} WHERE irn={}".format(table, self.irn))
        self.assertEqual(count, 0, '%s records for relationship %s are still present after deletion' % (count, relationship))

    @staticmethod
    def ensure_list(value):
        # Ensure a variable is a list & convert to a list if it's not
        return value if isinstance(value, list) else [value]



