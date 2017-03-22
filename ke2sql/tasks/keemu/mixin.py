
import time
import re
import logging
import abc
# from abc import ABCMeta, abstractproperty, abstractmethod
import luigi
import luigi.contrib.postgres

from ke2sql.lib.parser import Parser
from ke2sql.lib.config import Config
from ke2sql.tasks.keemu.file import FileTask


logger = logging.getLogger('luigi-interface')


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

    columns = [
        ("irn", "INTEGER PRIMARY KEY"),
        ("created", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
        ("modified", "TIMESTAMP"),
        ("deleted", "TIMESTAMP"),
        ("properties", "JSONB"),
        ("import_date", "INTEGER"),  # Date of import
    ]

    # Count total number of records (including skipped)
    record_count = 0
    # Count of records inserted / written to CSV
    insert_count = 0

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
        # Import the dataset tasks - needs to be here to prevent circular references
        from ke2sql.tasks.specimen import SpecimenDatasetTask
        from ke2sql.tasks.indexlot import IndexLotDatasetTask
        from ke2sql.tasks.artefact import ArtefactDatasetTask
        self.dataset_tasks = [SpecimenDatasetTask, IndexLotDatasetTask, ArtefactDatasetTask]
        # Get all fields and filters for this module
        # We will loop through all of the potentially applicable filters for a record,
        # And then use the corresponding field definitions to only include fields
        # Relevant to that particular dataset - we can therefore prevent index lots & artefacts
        # Getting a whole butch of extra specimen properties
        self.fields_per_filter = []
        for dataset_task in self.dataset_tasks:
            fields = [f for f in dataset_task.fields if f[0].split('.')[0] == self.module_name]
            filters = {}
            for filter_field, filter_ in dataset_task.filters.items():
                filter_module, filter_field_name = filter_field.split('.')
                if filter_module == self.module_name:
                    filters[filter_field_name] = filter_

            # If we have fields for this dataset, add to the list to be checked against
            if len(fields):
                self.fields_per_filter.append({
                    'filters': filters,
                    # The fields to use for a record matching this filter
                    'fields': fields
                })

        # print(filters)

                # filer_module, filter_field_name = filter_field.split('.')

        # Build a dictionary of record type fields & filters
        # We can then filter records by record type, and build
        # per record type property dicts
        # self.fields_and_filters_per_record_type = {}
        # for dataset_task in self.dataset_tasks:
        #     for record_type in dataset_task.record_types:
        #         self.fields_and_filters_per_record_type[record_type] = {
        #             'filters': dataset_task.filters
        #         }

        # print(self.record_types)
        # print(self.filters)

    #     # List of column field names
    #     self.columns_dict = dict(self.columns)
    #     # For faster processing, separate field mappings into those used
    #     # As properties (without a corresponding table column) and extra fields
    #     self._property_fields = []
    #     self._metadata_fields = []
    #     # Build a list of fields, separated into metadata and property
    #     for field in self.fields:
    #         # Is the field alias a defined column?
    #         if field[1] in self.columns_dict.keys():
    #             self._metadata_fields.append(field)
    #         else:
    #             self._property_fields.append(field)
    #
    #     # List of fields that are of type array
    #     self._array_fields = [col_name for col_name, col_def in self.get_column_types() if self._column_is_array(col_def)]
    #     # Set task ID so both Update & Copy tasks share the same identifier
    #     # If the copy task has run, the update task should be seen to be complete
    #     self.task_id = task_id_str(self.table, self.to_str_params(only_significant=True))

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

    # def get_filters(self):
    #     filters = {}
    #     for dataset_task in self.dataset_tasks:
    #         for filter_field, filter_ops in dataset_task.filters.items():
    #             filer_module, filter_field_name = filter_field.split('.')
    #             if filer_module == self.module_name:
    #                 if filter_field_name in filters:
    #                     # If we already have the field filtered on the same field, skip it
    #                     if filters[filter_field_name] == filter_ops:
    #                         continue
    #                     for filter_op in filter_ops:
    #
    #                         print('----')
    #                     # print(filters[filter_field_name])
    #                         print(filter_op)
    #                         print('----')
    #                 else:
    #                     filters[filter_field_name] = filter_ops
    #
    #     return filters

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
        record_dict = {
            'irn': record.irn,
            'properties': self.get_properties(record),
            'import_date': self.date
        }
        for (ke_field, alias) in self._metadata_fields:
            record_dict[alias] = getattr(record, ke_field, None)
            # Ensure value is of type list
            if record_dict[alias] and alias in self._array_fields and type(record_dict[alias]) != list:
                record_dict[alias] = [record_dict[alias]]
        return record_dict

    def get_properties(self, record):
        """
        Build dictionary of record properties
        If a field alias is an actual column, it will not be included in the property dict
        :param record:
        :return: dict
        """
        return {dataset_field: getattr(record, ke_field, None) for (ke_field, dataset_field) in self._property_fields if getattr(record, ke_field, None)}

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
