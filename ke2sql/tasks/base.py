
import time

import logging
from datetime import datetime
from abc import abstractproperty, abstractmethod
import luigi
import luigi.contrib.postgres

from ke2sql.lib.parser import Parser
from ke2sql.lib.config import Config
from ke2sql.tasks.file import FileTask


logger = logging.getLogger('luigi-interface')


class BaseTask(object):

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
        ("properties", "JSONB"),
        ("metadata", "JSONB"),
    ]

    # List of filters to check records against
    filters = {}

    # Count total number of records (including skipped)
    record_count = 0
    # Count of records inserted / written to CSV
    insert_count = 0

    # Fields to use
    metadata_mappings = []

    @abstractproperty
    def property_mappings(self):
        """
        List defining KE EMu fields to be included within a records' properties
        :return: List of tuples
        """
        return None

    @abstractproperty
    def table(self):
        """
        Table name
        :return:
        """
        return None

    @abstractmethod
    def delete_record(self, record):
        """
        Method for deleting records - Over-ridden in mixins
        :param record:
        :return:
        """
        pass

    def requires(self):
        return FileTask(module_name=self.table, date=self.date)

    def records(self):
        start_time = time.time()
        for record in Parser(self.input().path):
            self.record_count += 1
            if self._is_web_publishable(record) and self._apply_filters(record):
                self.insert_count += 1
                yield self.record_to_dict(record)
            else:
                self.delete_record(record)
            if self.limit and self.record_count >= self.limit:
                break
            if self.record_count % 1000 == 0:
                logger.debug('Record count: %d', self.record_count)
        logger.info('Inserted %d %s records in %d seconds', self.insert_count, self.table, time.time() - start_time)

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
                embargo_date_timestamp = time.mktime(datetime.strptime(embargo_date, "%Y-%m-%d").timetuple())
                if embargo_date_timestamp > today_timestamp:
                    return False

        return True

    def _apply_filters(self, record):
        """
        Apply any filters to exclude records based on any field filters
        See emultimedia::filters for example filters
        If any filters return False, the record will be skipped
        If all filters pass, will return True
        :return:
        """
        for field, filters in self.filters.items():
            value = getattr(record, field, None)
            for filter_operator, filter_value in filters:
                if not filter_operator(value, filter_value):
                    return False
        return True

    def record_to_dict(self, record):
        """
        Convert record object to a dict
        :param record:
        :return:
        """
        return {
            'irn': record.irn,
            'properties': self.get_properties(record),
            'metadata': self.get_metadata(record),
        }

    def get_properties(self, record):
        """
        Build dictionary of record properties
        :param record:
        :return: dict
        """
        return self._record_map_properties(record, self.property_mappings)

    def get_metadata(self, record):
        """
        Build dictionary of record properties
        :param record:
        :return: dict
        """
        metadata_dict = self._record_map_properties(record, self.metadata_mappings)
        metadata_dict['import_date'] = self.date
        return metadata_dict

    @staticmethod
    def _record_map_properties(record, properties):
        """
        Helper function - pass in a list of tuples
        (source field, destination field)
        And return a dict of values keyed by destination field
        :param record:
        :param properties:
        :return:
        """
        return {dest_prop: getattr(record, src_prop, None) for (src_prop, dest_prop) in properties if getattr(record, src_prop, None)}
