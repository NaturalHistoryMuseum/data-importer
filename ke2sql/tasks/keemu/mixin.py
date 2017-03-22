
import time
import re
import logging
import abc
# from abc import ABCMeta, abstractproperty, abstractmethod
import luigi
import luigi.contrib.postgres

from ke2sql.lib.parser import Parser
from ke2sql.lib.config import Config
from ke2sql.lib.helpers import get_dataset_tasks
from ke2sql.tasks.keemu.file import FileTask


logger = logging.getLogger('luigi-interface')


# FIXME: Copy VS Update
# FIXME: UPDATE ID
# CHOICE PARAM

# FIXME: Query
# FIXME: Create MAT VIEW
# FIXME: Delete


class KeemuMixin(object):
    """
    Mixin class for processing a KE EMu export file
    """
    date = luigi.IntParameter()
    limit = luigi.IntParameter(default=None, significant=False)
    module_name = luigi.ChoiceParameter(choices=['ecatalogue', 'emultimedia', 'etaxonomy'])

    # Luigi Postgres database connections
    host = Config.get('database', 'host')
    database = Config.get('database', 'database')
    user = Config.get('database', 'username')
    password = Config.get('database', 'password')

    # Count total number of records (including skipped)
    record_count = 0
    # Count of records inserted / written to CSV
    insert_count = 0

    columns = [
        ("irn", "INTEGER PRIMARY KEY"),
        ("created", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
        ("modified", "TIMESTAMP"),
        ("deleted", "TIMESTAMP"),
        ("properties", "JSONB"),
        ("import_date", "INTEGER"),  # Date of import
    ]

    @abc.abstractproperty
    def fields(self):
        """
        List defining KE EMu fields and their aliases
        :rtype: list
        :return: List of tuples
        """
        return []

    @property
    def table(self):
        """
        By default table name is just module name
        :return: string
        """
        return self.module_name

    @abc.abstractmethod
    def delete_record(self, record):
        """
        Method for deleting records - Over-ridden in mixins
        :param record:
        :return: None
        """

    def __init__(self, *args, **kwargs):
        super(KeemuMixin, self).__init__(*args, **kwargs)
        # Get all fields and filters for this module
        # We will loop through all of the potentially applicable filters for a record,
        # And then use the corresponding field definitions to only include fields
        # Relevant to that particular dataset - we can therefore prevent index lots & artefacts
        # Getting a whole butch of extra specimen properties
        self.dataset_filters = []
        for dataset_task in get_dataset_tasks():
            dataset_filter = {
                # List of fields matching the module name, with the module name removed
                'fields': [(f.field_name, f.field_alias) for f in dataset_task.fields if f.module_name == self.module_name],
                'filters': {},
                'metadata_fields': [],
                'metadata_array_fields': [],
            }
            # If we don't have any fields at this point, continue to next dataset
            if not len(dataset_filter['fields']):
                continue

            for dataset_task_filter in dataset_task.filters:
                if dataset_task_filter.module_name == self.module_name:
                    dataset_filter['filters'][dataset_task_filter.field_name] = dataset_task_filter.filters

            # Add all the metadata fields to the table columns
            # Which is used to create the base table
            for metadata_field in dataset_task.metadata_fields:
                # Only add the field if it matches the module name being imported
                if metadata_field.module_name == self.module_name:
                    col = (metadata_field.field_alias, metadata_field.field_type)
                    if col not in self.columns:
                        self.columns.append(col)
                    # We also want to add this to the metadata fields list, used for building the record
                    dataset_filter['metadata_fields'].append((metadata_field.field_name, metadata_field.field_alias))
                    # Keep a not of all extra metadata fields that need to be arrays
                    if self._column_is_array(metadata_field.field_type):
                        dataset_filter['metadata_array_fields'].append(metadata_field.field_alias)

            self.dataset_filters.append(dataset_filter)

    #     self.task_id = task_id_str(self.table, self.to_str_params(only_significant=True))

    def requires(self):
        return FileTask(module_name=self.table, date=self.date)

    def records(self):
        start_time = time.time()
        for record in Parser(self.input().path):
            self.record_count += 1
            if self._is_web_publishable(record):
                # Loop through all the dataset filters - if one passes
                # we can then use the corresponding fields to build a record dict
                # If non passes then we skip the record
                for dataset_filter in self.dataset_filters:
                    if self._apply_filters(dataset_filter['filters'], record):
                        self.insert_count += 1
                        yield self.record_to_dict(record, dataset_filter)
                        # Break out of dataset filter loop as soon as a record
                        # passes through the filters
                        break
            else:
                # Just in case a record has been marked Non web publishable,
                # Try and delete the record
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
        return record.AdmPublishWebNoPasswordFlag.lower() == 'y'

    @staticmethod
    def _apply_filters(dataset_filters, record):
        """
        Apply any filters to exclude records based on any field filters
        See emultimedia::filters for example filters
        If any filters return False, the record will be skipped
        If all filters pass, will return True
        :return:
        """
        for field, filters in dataset_filters.items():
            value = getattr(record, field, None)
            for filter_operator, filter_value in filters:
                if not filter_operator(value, filter_value):
                    return False
        return True

    def record_to_dict(self, record, dataset_filter):
        """
        Convert record object to a dict
        :param record:
        :param dataset_filter:
        :return:
        """
        record_dict = {
            'irn': record.irn,
            'properties': self._record_map_fields(record, dataset_filter['fields']),
            'import_date': self.date
        }
        # Get any extra metadata fields, and add it the record dict
        metadata = self._record_map_fields(record, dataset_filter['metadata_fields'])
        # If we have any metadata array fields, ensure they are indeed arrays
        for metadata_array_field in dataset_filter['metadata_array_fields']:
            if metadata.get(metadata_array_field, None) and type(metadata[metadata_array_field]) != list:
                metadata[metadata_array_field] = [metadata[metadata_array_field]]

        record_dict.update(metadata)
        return record_dict

    @staticmethod
    def _record_map_fields(record, fields):
        """
        Helper function - pass in a list of tuples
        (source field, destination field)
        And return a dict of values keyed by destination field
        :param record:
        :param fields:
        :return:
        """
        return {dataset_field: getattr(record, ke_field, None) for (ke_field, dataset_field) in fields if getattr(record, ke_field, None)}

    def get_column_types(self):
        """
        Return a dict of column types, keyed by column name
        :return:
        """
        regex = re.compile('(^[A-Z]+(\[\])?)')
        return [(column_name, regex.match(column_def).group(1)) for column_name, column_def in self.columns]

    @staticmethod
    def _column_is_array(col_def):
        """
        Evaluate whether a column is an array (has [])
        :return:
        """
        return '[]' in col_def
