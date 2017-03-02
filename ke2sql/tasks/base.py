import os
import re
import abc
import gzip
import luigi
import time
import datetime
import logging

from psycopg2.extras import Json

from ke2sql.lib.parser import Parser
from ke2sql.lib.config import Config
from ke2sql.tasks.file import FileTask
import ke2sql.lib.db as db

logger = logging.getLogger('luigi-interface')


class BaseTask(luigi.Task):

    date = luigi.IntParameter()
    limit = luigi.IntParameter(default=None)

    # Count total number of records (including skipped)
    record_count = 0
    # Count of records inserted / written to CSV
    insert_count = 0
    # Buffer for holding records so they are not individually inserted
    buffer = []
    # Maximum number of records to hold in the buffer before writing
    buffer_size = 1000

    def __init__(self, *args, **kwargs):
        # Initiate a DB connection
        super(BaseTask, self).__init__(*args, **kwargs)
        # Initiate the DB tables
        db.init()
        # Set up connections and model
        self.connection = db.get_connection()
        self.model = self.model_cls()
        # Boolean denoting if this is the latest full export being run
        # If it is we can skip certain parts of the process - deleting records etc.,
        self.full_import = self.date == Config.getint('keemu', 'full_export_date')

    @abc.abstractproperty
    def model_cls(self):
        return None

    @property
    def module(self):
        """
        Module - lower case class with model removed
        :return:
        """
        return self.__class__.__name__.lower().replace('task', '')

    def requires(self):
        return FileTask(module_name=self.module, date=self.date)

    def run(self):
        """
        Main run task - process the records
        """

        start_time = time.time()
        parser = Parser(self.input().path)
        for record in parser:
            self.record_count += 1
            if self._is_web_publishable(record) and self._is_valid_record(record):
                # Get an extra fields
                record_dict = {
                    'irn': record.irn,
                    'properties': Json(record.to_dict(self.model.property_mappings))
                }
                # Add any extra fields
                for extra_field in self.model.get_extra_fields():
                    field_name = extra_field.alias if extra_field.alias else extra_field.key
                    record_dict[extra_field.key] = getattr(record, field_name)
                # Once record dict is built, add the record to the buffer
                self.buffer.append(record_dict)
                self.insert_count += 1
            # If this isn't a full import try and delete the record
            # Only delete if the record is valid - otherwise we'll be trying to
            # Delete a load of cruft
            elif not self.full_import and self._is_valid_record(record):
                self.delete_record(record)

            if len(self.buffer) == self.buffer_size:
                self.write_buffer()

            if self.limit and self.record_count >= self.limit:
                break

            if self.record_count % 1000 == 0:
                logger.debug('Record count: %d', self.record_count)

        # After last loop, write records if we still have entries in the buffer
        if len(self.buffer):
            self.write_buffer()

        logger.info('Inserted %d %s records in %d seconds', self.insert_count, self.module, time.time() - start_time)

    def write_buffer(self):
        """
        Write buffer to database
        :return: None
        """
        logger.info('Writing buffer - %d (%d)', self.insert_count, self.record_count)
        # Write contents on the buffer to DB
        self.connection.execute(self.model.sql, self.buffer)
        # Reset the buffer
        self.buffer = []

    def delete_record(self, record):
        """
        Marks a record as deleted
        :return: None
        """
        # print("DELETE")
        self.connection.execute(self.model.delete_sql, irn=record.irn)

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

    def _is_valid_record(self, record):
        """
        Validate whether a record has all the correct fields and values
        :param record:
        :return: boolean - true if record passes validation
        """
        return True

