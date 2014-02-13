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

class BaseTask(object):

    file_name = 'export'
    export_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')

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
    file_name = 'export'

    @abc.abstractproperty
    def task(self):
        return None

    @abc.abstractproperty
    def model(self):
        return None

    def test_import(self):
        luigi.build([self.task()], local_scheduler=True)
        obj = self.session.query(self.model).filter(self.model.irn == 1).one()

    def test_data(self):

        """
        Ensure the data in the file matches what is in the DB
        """

        keemu_schema_file = config.get('keemu', 'schema')

        #  Find the path to the import file from the import task
        task = self.task()

        # TODO: This will fail on catalogue
        file_task = task.requires()[0]
        file_path = file_task.get_file_path()
        ke_data = KEParser(open(file_path, 'r'), schema_file=keemu_schema_file, input_file_path=file_path)

        # Load the obj from the database
        obj = self.session.query(self.model).filter(self.model.irn == 1).one()

        # Get all the field aliases
        aliases = self.model().get_aliases()

        for data in ke_data:

            for field, value in data.items():
                db_field = aliases[field] if field in aliases else field
                # Assert the values are equal
                # TODO: This fails on nested data
                self.assertEqual(getattr(obj, db_field), value)


    def test_delete(self):
        # Ensure the data is there to be deleted
        self.test_import()
        obj = self.session.query(self.model).filter(self.model.irn == 1).one()

        # Delete the obj
        self.session.delete(obj)
        self.session.commit()
        count = self.session.query(self.model).filter(self.model.irn == 1).count()
        self.assertEqual(count, 0)
