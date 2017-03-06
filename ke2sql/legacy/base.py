import os
import re
import abc
import gzip
import luigi
import luigi.contrib.postgres
import time
from datetime import datetime
import datetime
import logging
import json
from abc import abstractproperty

from ke2sql.lib.parser import Parser
from ke2sql.lib.config import Config
from ke2sql.tasks.file import FileTask

logger = logging.getLogger('luigi-interface')


class BaseTask(luigi.contrib.postgres.CopyToTable):

    date = luigi.IntParameter()
    limit = luigi.IntParameter(default=None)

    # Luigi Postgres database connections
    host = Config.get('database', 'host')
    database = Config.get('database', 'database')
    user = Config.get('database', 'username')
    password = Config.get('database', 'password')

    columns = [
        ("irn", "INTEGER PRIMARY KEY"),
        ("created", "TIMESTAMP NOT NULL"),
        ("modified", "TIMESTAMP"),
        ("deleted", "TIMESTAMP"),
        ("properties", "JSONB")
    ]

    # List of filters to check records against
    filters = []

    # Count total number of records (including skipped)
    record_count = 0
    # Count of records inserted / written to CSV
    insert_count = 0

    @abstractproperty
    def property_mappings(self):
        """
        List defining KE EMu fields to be included within a records' properties
        :return: List of tuples
        """
        return None

    @property
    def table(cls):
        """
        Table name - lower case class with model removed
        :return:
        """
        return cls.__class__.__name__.lower().replace('task', '')

    def __init__(self, *args, **kwargs):
        # Initiate a DB connection
        super(BaseTask, self).__init__(*args, **kwargs)
        # Boolean denoting if this is the latest full export being run
        # If it is we can skip certain parts of the process - deleting records etc.,
        self.full_import = (self.date == Config.getint('keemu', 'full_export_date'))

    def requires(self):
        return FileTask(module_name=self.table, date=self.date)

    def rows(self):
        """
        Implementation of CopyToTable.rows()
        :return:
        """
        start_time = time.time()
        for record in Parser(self.input().path):
            self.record_count += 1
            # TODO: Add filters
            if self._is_web_publishable(record):
                self.insert_count += 1
                row = self.get_row(record)
                yield row
            else:
                self.delete_record(record)
            if self.limit and self.record_count >= self.limit:
                break
            if self.record_count % 1000 == 0:
                logger.debug('Record count: %d', self.record_count)

        logger.info('Inserted %d %s records in %d seconds', self.insert_count, self.table, time.time() - start_time)

    def get_row(self, record):
        return [
            int(record.irn),  # IRN
            None,  # Date Created
            None,  # Date Updated
            None,  # Date Deleted
            json.dumps(record.to_dict(self.property_mappings)),  # Properties
        ]

    def delete_record(self, record):
        """
        Marks a record as deleted
        :return: None
        """
        # print("DELETE")
        # self.connection.execute(self.model.delete_sql, irn=record.irn)
        pass

    @staticmethod
    def _is_web_publishable(record):
        """
        Evaluate whether a record is importable
        At the very least a record will need AdmPublishWebNoPasswordFlag set to Y,
        Additional models will extend this to provide additional filters
        :param record:
        :return: boolean - false if not importable
        """
        if record.AdmPublishWebNoPasswordFlag.lower() != 'y':
            return False

        today_timestamp = time.time()
        embargo_dates = [
            getattr(record, 'NhmSecEmbargoDate', None),
            getattr(record, 'NhmSecEmbargoExtensionDate', None)
        ]
        for embargo_date in embargo_dates:
            if embargo_date:
                embargo_date_timestamp = time.mktime(datetime.datetime.strptime(embargo_date, "%Y-%m-%d").timetuple())
                if embargo_date_timestamp > today_timestamp:
                    return False

        return True

