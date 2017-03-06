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
from psycopg2.extras import Json

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
        ("created", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
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
    def table(self):
        """
        Table name - lower case class with model removed
        :return:
        """
        return self.__class__.__name__.lower().replace('task', '')



    def __init__(self, *args, **kwargs):
        # Initiate a DB connection
        super(BaseTask, self).__init__(*args, **kwargs)
        # Boolean denoting if this is the latest full export being run
        # If it is we can use the copy to file process (which is much faster)
        self.file_copy = (self.date == Config.getint('keemu', 'full_export_date'))

    def requires(self):
        return FileTask(module_name=self.table, date=self.date)

    def run(self):
        # If this is being rebuild from the full_import, then use the default CopyToTable.run
        if self.file_copy:
            logger.debug('Bulk loading records')
            super(BaseTask, self).run()
        else:
            logger.debug('Updating records')
            connection = self.output().connect()
            cursor = connection.cursor()
            for record in self.records():
                # psycopg2 encode Json
                record['properties'] = Json(record['properties'])
                cursor.execute(self.insert_sql, record)

    def records(self):
        start_time = time.time()
        for record in Parser(self.input().path):
            self.record_count += 1
            if self._is_web_publishable(record):
                self.insert_count += 1
                yield {
                    'irn': record.irn,
                    'properties': record.to_dict(self.property_mappings)
                }
            else:
                self.delete_record(record)
            if self.limit and self.record_count >= self.limit:
                break
            if self.record_count % 1000 == 0:
                logger.debug('Record count: %d', self.record_count)
        logger.info('Inserted %d %s records in %d seconds', self.insert_count, self.table, time.time() - start_time)

    def rows(self):
        """
        Implementation of CopyToTable.rows()
        :return:
        """
        for record in self.records():
            row = (
                int(record['irn']),  # IRN
                int(time.time()),  # Date Created
                None,  # Date Updated
                None,  # Date Deleted
                json.dumps(record['properties']),  # Properties
            )
            print(row)
            yield row

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

