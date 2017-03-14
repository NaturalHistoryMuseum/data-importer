
import time
import re
import logging
from datetime import datetime
from abc import abstractproperty, abstractmethod
import luigi
import luigi.contrib.postgres
from luigi.task import task_id_str
from ke2sql.lib.parser import Parser
from ke2sql.lib.config import Config
from ke2sql.tasks.file import FileTask


logger = logging.getLogger('luigi-interface')


class BaseTask(object):

    date = luigi.IntParameter()
    limit = luigi.IntParameter(default=None, significant=False)

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

    # List of filters to check records against
    filters = {}

    # Count total number of records (including skipped)
    record_count = 0
    # Count of records inserted / written to CSV
    insert_count = 0

    @property
    def table(self):
        """
        Table name - lower case class name up to third capital
        ECatalogueCopyTask => ecatalogue
        ECatalogueUpdateTask => ecatalogue
        ECatalogueTask => ecatalogue
        :return:
        """
        m = re.match('(^[A-Z]{2}[a-z]+)',  self.__class__.__name__)
        return m.group(1).lower()

    @abstractproperty
    def field_mappings(self):
        """
        List defining KE EMu fields and their aliases
        :return: List of tuples
        """
        return {}

    @abstractmethod
    def delete_record(self, record):
        """
        Method for deleting records - Over-ridden in mixins
        :param record:
        :return:
        """
        pass

    def __init__(self, *args, **kwargs):
        # Initiate a DB connection
        super(BaseTask, self).__init__(*args, **kwargs)
        # List of column field names
        self.columns_dict = dict(self.columns)
        # For faster processing, separate field mappings into those used
        # As properties (without a corresponding table column) and extra fields
        self._property_field_mappings = []
        self._extra_field_mappings = []
        for field_mapping in self.field_mappings:
            # Is the field alias a defined column?
            if field_mapping[1] in self.columns_dict.keys():
                self._extra_field_mappings.append(field_mapping)
            else:
                self._property_field_mappings.append(field_mapping)
        # List of fields that are of type array
        self._array_fields = [col_name for col_name, col_def in self.get_column_types() if self._column_is_array(col_def)]
        # Set task ID so both Update & Copy tasks share the same identifier
        # If the copy task has run, the update task should be seen to be complete
        self.task_id = task_id_str(self.table, self.to_str_params(only_significant=True))

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
        record_dict = {
            'irn': record.irn,
            'properties': self.get_properties(record),
            'import_date': self.date
        }
        for (ke_field, alias) in self._extra_field_mappings:
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
        return {alias: getattr(record, ke_field, None) for (ke_field, alias) in self._property_field_mappings if getattr(record, ke_field, None)}

    def get_column_types(self):
        """
        Return a dict of column types, keyed by column name
        :return:
        """
        regex = re.compile('(^[A-Z]+(\[\])?)')
        return [(column_name, regex.match(column_def).group(1)) for column_name, column_def in self.columns]

    def ensure_indexes(self, connection):
        cursor = connection.cursor()
        for col_name, col_def in self.get_column_types():
            # Don't create index on irn or properties column
            if col_name in ['irn', 'properties']:
                continue
            query = "CREATE INDEX ON {table} USING {idx_type} ({col_name})".format(
                table=self.table,
                # Use a GIN index on array column types; otherwise BTREE
                idx_type='GIN' if self._column_is_array(col_def) else 'BTREE',
                col_name=col_name
            )
            cursor.execute(query)

    def table_exists(self, connection):
        cursor = connection.cursor()
        cursor.execute("select exists(select * from information_schema.tables where table_name=%s)", (self.table,))
        return cursor.fetchone()[0]

    def drop_table(self, connection):
        """
        Drop the table
        :param connection:
        :return:
        """
        query = "DROP TABLE IF EXISTS {table}".format(table=self.table)
        connection.cursor().execute(query)
        connection.commit()

    @staticmethod
    def _column_is_array(col_def):
        """
        Evaluate whether a column is an array (has [])
        :return:
        """
        return '[]' in col_def
