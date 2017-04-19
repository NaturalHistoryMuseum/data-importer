#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '31/03/2017'.
"""

import os
import luigi
import unittest
import psycopg2

from ke2sql.lib.config import Config
from ke2sql.lib.helpers import get_dataset_tasks, list_all_modules, get_file_export_dates
from ke2sql.tasks.delete import DeleteTask
from ke2sql.tests.test_task import TestTask


class BaseTestCase(unittest.TestCase):

    connection = None
    cursor = None

    @classmethod
    def setUpClass(cls):
        cls.connection = psycopg2.connect(
            host=Config.get('database', 'host'),
            port=Config.get('database', 'port'),
            database=Config.get('database', 'datastore_dbname'),
            user=Config.get('database', 'username'),
            password=Config.get('database', 'password')
        )
        cls.cursor = cls.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        # Run test tasks for all the export dates
        for export_date in get_file_export_dates():
            luigi.build([TestTask(date=int(export_date))], local_scheduler=True)

    def _get_record(self, table_name, **kwargs):
        """
        Retrieve a record, building where clause from kwargs
        :param table_name:
        :param kwargs:
        :return:
        """
        sql = 'SELECT * FROM "{}" WHERE {}'.format(
            table_name,
            ' AND '.join(['{} = %s'.format(k) for k in kwargs.keys()])
        )
        self.cursor.execute(sql, list(kwargs.values()))
        return self.cursor.fetchone()

    def _get_dataset_record(self, resource_name, irn):
        table_name = Config.get('resource_ids', resource_name)
        return self._get_record(table_name, _id=irn)

    def assertRecordExists(self, table_name, irn):
        record = self._get_record(table_name, irn=irn)
        self.assertEqual(irn, record['irn'])

    def assertDatasetRecordExists(self, dataset_name, irn):
        record = self._get_dataset_record(dataset_name, irn)
        self.assertEqual(irn, record['_id'])

    def assertRecordDoesNotExist(self, table_name, irn):
        record = self._get_record(table_name, irn=irn)
        self.assertIsNone(record)

    def assertDatasetRecordDoesNotExist(self, dataset_name, irn):
        record = self._get_dataset_record(dataset_name, irn)
        self.assertIsNone(record)

    def assertRecordIsDeleted(self, table_name, irn):
        record = self._get_record(table_name, irn=irn)
        self.assertIsNotNone(record['deleted'])

    @classmethod
    def tearDownClass(cls):
        # Delete all table updates
        cls.cursor.execute('DELETE FROM table_updates')
        # Delete all info in the module tables
        for module_name in list_all_modules():
            cls.cursor.execute('DELETE FROM "{module_name}"'.format(
                module_name=module_name
            ))
        cls.connection.commit()
        cls.connection.close()
