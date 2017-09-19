#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '19/09/2017'.
"""

import luigi
from luigi.contrib.postgres import PostgresTarget
from data_importer.lib.config import Config


class PostgresTask(luigi.Task):
    """
    Basic Luigi task for setting up a connection to Postgres
    """

    # Luigi Postgres database connections
    host = Config.get('database', 'host')
    database = Config.get('database', 'datastore_dbname')
    user = Config.get('database', 'username')
    password = Config.get('database', 'password')

    def output(self):
        """
        Returns a PostgresTarget representing the inserted dataset.

        Normally you don't override this.
        """
        return PostgresTarget(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            table=self.table,
            update_id=self.task_id
        )