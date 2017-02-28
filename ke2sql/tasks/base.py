import os
import re
import abc
import gzip
import luigi
import time
import datetime

from psycopg2.extras import Json
from sqlalchemy.orm import class_mapper


from ke2sql.lib.parser import Parser
from ke2sql.tasks.file import FileTask
import ke2sql.lib.db as db


class BaseTask(luigi.Task):

    date = luigi.IntParameter()

    # Count total number of records (including skipped)
    record_count = 0
    # Count of records inserted / written to CSV
    insert_count = 0
    # Buffer for holding records so they are not individually inserted
    buffer = []
    # Maximum number of records to hold in the buffer before writing
    buffer_size = 10

    def __init__(self, *args, **kwargs):
        # Initiate a DB connection
        super(BaseTask, self).__init__(*args, **kwargs)
        # Initiate the DB tables
        db.init()
        # Set up connections and model
        self.connection = db.get_connection()
        self.model = self.model_cls()

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

        print(self.model.get_extra_fields())
        return

        parser = Parser(self.input().path)
        for record in parser:
            self.record_count += 1
            if self._is_importable(record):

                # Get an extra fields

                self.buffer.append({
                    'irn': record.irn,
                    'properties': Json(record.to_dict(self.model.property_mappings))
                })
                self.insert_count += 1
            else:
                self.delete_record()

            if len(self.buffer) == self.buffer_size:
                self.write_buffer()

        # After last loop, write records if we still have entries in the buffer
        if len(self.buffer):
            self.write_buffer()

    def write_buffer(self):
        """
        Write buffer to database
        :return: None
        """
        print('Writing buffer - {0} ({1})'.format(self.insert_count, self.record_count))
        # Write contents on the buffer to DB
        self.connection.execute(self.model.sql, self.buffer)
        # Reset the buffer
        self.buffer = []

    def delete_record(self):
        """
        Marks a record as deleted
        :return: None
        """

    def _is_importable(self, record):
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
