#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '30/03/2017'.
"""

import luigi
import logging
from data_importer.tasks.postgres import PostgresTask
from data_importer.tasks.file.keemu import KeemuFileTask
from data_importer.lib.parser import Parser
from data_importer.lib.dataset import dataset_get_tasks
from data_importer.lib.db import db_delete_record

logger = logging.getLogger('luigi-interface')


class DeleteTask(PostgresTask):

    # Task params
    date = luigi.IntParameter()
    table = 'eaudit'

    # Run delete before all dataset tasks
    priority = 100

    def requires(self):
        return KeemuFileTask(
            file_name='eaudit.deleted-export',
            date=self.date
        )

    @staticmethod
    def list_all_modules():
        """
        Build a list of all unique module names
        :return:
        """
        dataset_tasks = dataset_get_tasks()
        modules = set()
        [[modules.add(field.module_name) for field in dataset_task.fields] for dataset_task in dataset_tasks]
        return list(modules)

    def run(self):
        logger.info('Executing task: {name}'.format(name=self.__class__))
        modules = self.list_all_modules()
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