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
    irn = 1

    @abc.abstractproperty
    def task(self):
        return None

    @abc.abstractproperty
    def model(self):
        return None

    # def test_import(self):
    #     self.create()
    #     luigi.build([self.task()], local_scheduler=True)
    #     obj = self.query().one()
    #     # self.delete()
    #
    # def test_data(self):
    #
    #     """
    #     Ensure the data in the file matches what is in the DB
    #     """
    #
    #     self.create()
    #     keemu_schema_file = config.get('keemu', 'schema')
    #
    #     #  Find the path to the import file from the import task
    #     task = self.task()
    #
    #     # TODO: This will fail on catalogue
    #     file_task = task.requires()[0]
    #     file_path = file_task.get_file_path()
    #     ke_data = KEParser(open(file_path, 'r'), schema_file=keemu_schema_file, input_file_path=file_path)
    #
    #     # Load the obj from the database
    #     obj = self.query().one()
    #
    #     # Get all the field aliases
    #     aliases = self.model().get_aliases()





        # for data in ke_data:
        #
        #     for field, value in data.items():
        #         db_field = aliases[field] if field in aliases else field
        #         # Assert the values are equal - only works for flat fields
        #         # Other cases are handled in the module/file specific tests
        #         if hasattr(obj, db_field):
        #
        #             obj_value = getattr(obj, db_field)
        #
        #             # Convert dates to unicode for comparison
        #             if isinstance(obj_value, date):
        #                 obj_value = unicode(obj_value)
        #
        #             self.assertEqual(obj_value, value, '%s does not match %s %s' % (db_field, obj_value, value))

        # Destroy the object
        # self.delete()


    # def test_delete(self):
    #     # Ensure the data is there to be deleted
    #     self.create()
    #     # ANd then delete it
    #     self.delete()
    #     count = self.query().count()
    #     self.assertEqual(count, 0)

    def create(self):
        luigi.build([self.task()], local_scheduler=True)

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
        task = self.task()

        # TODO: This will fail on catalogue
        file_task = task.requires()[0]
        file_path = file_task.get_file_path()
        return KEParser(open(file_path, 'r'), schema_file=keemu_schema_file, input_file_path=file_path)

    def test_relationship(self):
        obj = self.query().one()
        data = self.get_data().next()

        print obj.material_detail

        x = 0
        for material in obj.material_detail:

            aliases = material.get_aliases()

            for alias, db_field in aliases.items():
                obj_value = getattr(material, db_field)

                print data.get(alias, None)[x]

                # self.assertEqual(obj_value, data.get(alias), '%s does not match %s %s' % (db_field, obj_value, value))

                # print data[alias]
                #
                # print obj_value

            x += 1


            # print aliases
            #
            # print material.irn

        print 'test'



