#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '31/03/2017'.
"""

import os
import luigi
import unittest
import psycopg2
from psycopg2.extras import DictCursor

from ke2sql.lib.config import Config
from ke2sql.lib.helpers import get_dataset_tasks, list_all_modules


class BaseTestCase(unittest.TestCase):

    test_export_date = '20170101'

    def setUp(self):
        self.connection = psycopg2.connect(
            host=Config.get('database', 'host'),
            port=Config.get('database', 'port'),
            database=Config.get('database', 'datastore_dbname'),
            user=Config.get('database', 'username'),
            password=Config.get('database', 'password')
        )
        self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        params = {
            'date': self.test_export_date,
        }
        self.tasks = []
        for task in get_dataset_tasks():
            self.tasks.append(task(**params))
        luigi.build(self.tasks, local_scheduler=True)

    def _get_record(self, table_name, **kwargs):

        # FIXME: Change this
        field_name = list(kwargs.keys())[0]
        sql = 'SELECT * FROM "{table_name}" WHERE {field_name} = %s'.format(
            field_name=field_name,
            table_name=table_name,
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

    def tearDown(self):
        # Delete all table updates
        self.cursor.execute('DELETE FROM table_updates')
        # Delete all info in the module tables
        # for module_name in list_all_modules():
        #     self.cursor.execute('DELETE FROM "{module_name}"'.format(
        #         module_name=module_name
        #     ))
        self.connection.commit()
        self.connection.close()
