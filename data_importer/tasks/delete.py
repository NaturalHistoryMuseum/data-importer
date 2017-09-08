#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '30/03/2017'.
"""

import luigi
import logging
from luigi.contrib.postgres import PostgresQuery, PostgresTarget

from data_importer.lib.config import Config
from data_importer.tasks.keemu.file import FileTask
from data_importer.lib.parser import Parser
from data_importer.lib.helpers import list_all_modules
from data_importer.lib.db import db_delete_record

logger = logging.getLogger('luigi-interface')


class DeleteTask(PostgresQuery):

    # Task params
    date = luigi.IntParameter()

    # Luigi Postgres database connections
    host = Config.get('database', 'host')
    database = Config.get('database', 'datastore_dbname')
    user = Config.get('database', 'username')
    password = Config.get('database', 'password')
    table = 'eaudit'
    query = None

    # Run delete before all dataset tasks
    priority = 100

    def requires(self):
        return FileTask(
            file_name='eaudit.deleted-export',
            date=self.date
        )

    def run(self):
        logger.info('Executing task: {name}'.format(name=self.__class__))
        modules = list_all_modules()
        connection = self.output().connect()
        cursor = connection.cursor()
        for record in Parser(self.input().path):
            module_name = record.AudTable
            # We only want to delete a record, if we're using information
            # from the module
            if module_name not in modules:
                # Skip record if it's one of the  modules
                # we're not using
                continue

            irn = record.AudKey
            db_delete_record(module_name, irn, cursor)

        # mark as complete in same transaction
        self.output().touch(connection)

        # commit and close connection
        connection.commit()
        connection.close()


if __name__ == "__main__":
    luigi.run(main_task_cls=DeleteTask)