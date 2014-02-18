#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import sys
import os
import luigi
from ke2sql.model import meta
from ke2sql.tasks.ke import KEFileTask
import abc
from datetime import date, datetime
from sqlalchemy.orm import class_mapper


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

    def test_update(self):
        """
        Import the data and check it's there
        """
        self.create()
        # Rerun with the update import
        self.file_name = 'update'
        self.create()
        self.delete()

    def test_update(self):
        self.update()
        self.create()
        self.delete()

    def test_created_date(self):
        self.create()
        obj = self.query().one()
        # New record so updated and inserted should be equal
        self.assertEquals(obj._modified, obj._created, 'Created date is not the same as created date')
        for t in ('year', 'month', 'day', 'hour', 'minute'):
            now = datetime.now()
            self.assertEqual(getattr(obj._created, t), getattr(now, t), 'Created date %s is wrong' % t)

        self.delete()

    def test_updated_date(self):
        self.update()
        self.session.commit()
        self.create()
        obj = self.query().one()
        # Make sure updated is different to created date
        self.assertNotEquals(obj._modified, obj._created, 'Updated date is the same as created date')

        # Make sure updated time is now
        for t in ('year', 'month', 'day', 'hour', 'minute'):
            now = datetime.now()
            self.assertEqual(getattr(obj._modified, t), getattr(now, t), 'Updated date %s is wrong' % t)

        self.delete()

    def test_not_null(self):
        """
        All fields should be populated
        """
        self.create()
        obj = self.query().one()
        for prop in class_mapper(self.model).iterate_properties:



            self.assertIsNotNone(getattr(obj, prop.key), 'Property %s is None' % prop)

        # self.delete()


    def update(self):
        """
        Helper function for running updates
        Just run once and then rename the import file
        """
        self.create()
        self.file_name = self.file_name.replace('export', 'update')
        return self.create()

    def create(self):
        luigi.build([self.task(file_name=self.file_name)], local_scheduler=True)
        return self.query().one()

    def delete(self):
        obj = self.query().one()
        # Delete the obj
        self.session.delete(obj)
        self.session.commit()

    def query(self):
        return self.session.query(self.model).filter(self.model.irn == self.irn)

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
        self.delete()

    def _test_relationship_not_exists(self, relationship):
        """
        Check a relationship exists
        """
        self.create()
        obj = self.query().one()
        value = getattr(obj, relationship)
        # Check we have the object
        self.assertEqual(value, [], 'Relationship %s should not exist' % relationship)

    def _test_relationship_delete(self, relationship, table=None, key=None):
        """
        For a given relationship, check all records in the associated tables have been deleted
        """
        self.create()
        self.delete()

        if not table:
            table = getattr(self.model, relationship).property.table

        if not key:
            key = 'irn'

        count = self.session.scalar("SELECT COUNT(*) FROM {} WHERE {}={}".format(table, key, self.irn))
        self.assertEqual(count, 0, '%s records for relationship %s are still present after deletion' % (count, relationship))



