#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '03/03/2017'.
"""

import abc
import logging
from psycopg2.extras import Json as PGJson
import luigi
import time
from operator import is_not
from luigi.contrib.postgres import CopyToTable as LuigiCopyToTable
from data_importer.lib.db import db_table_exists, db_delete_record, db_create_index
from data_importer.lib.operators import is_not_one_of, is_uuid
from data_importer.tasks.file.keemu import KeemuFileTask
from data_importer.lib.parser import Parser
from data_importer.lib.config import Config
from data_importer.lib.column import Column
from data_importer.lib.filter import Filter

from data_importer.lib.dataset import (
    dataset_get_foreign_keys,
    dataset_get_properties
)

from data_importer.lib.stats import (
    get_milestones
)

logger = logging.getLogger('luigi-interface')


class KeemuBaseTask(LuigiCopyToTable):
    """
    Extends CopyToTable to write directly to database using Upsert statements
    """
    # Task parameters
    date = luigi.IntParameter()
    limit = luigi.IntParameter(default=None, significant=False)

    @abc.abstractproperty
    def module_name(self):
        """
        Name of the KE EMu module - set in ecatalogue etc.,
        Base classes deriving form this one
        """
        return []

    # Base postgres columns for importing a keemu module
    # These can be extended by individual module task classes
    columns = [
        Column("irn", "INTEGER PRIMARY KEY"),
        Column("guid", "UUID", indexed=True),
        Column("created", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
        Column("modified", "TIMESTAMP"),
        Column("deleted", "TIMESTAMP", indexed=True),
        Column("properties", "JSONB", indexed=True),  # The field for storing the record data
        Column("import_date", "INTEGER", indexed=True),  # Date of import
    ]

    # Filters to apply against each record.
    # If any return False, the record will not be imported
    record_filters = [
        Filter('AdmPublishWebNoPasswordFlag', [
            (is_not_one_of, ['n', 'N'])
        ]),
        Filter('AdmGUIDPreferredValue', [
            (is_not, None),
            is_uuid
        ]),
    ]

    # Luigi Postgres database connections
    host = Config.get('database', 'host')
    database = Config.get('database', 'datastore_dbname')
    user = Config.get('database', 'username')
    password = Config.get('database', 'password')

    @property
    def table(self):
        """
        Table name is just module name, but is required by LuigiCopyToTable
        :return: string
        """
        return self.module_name

    # Count total number of records (including skipped)
    record_count = 0
    # Count of records inserted / written to CSV
    insert_count = 0

    def __init__(self, *args, **kwargs):
        # Initiate a DB connection
        super(KeemuBaseTask, self).__init__(*args, **kwargs)
        self.connection = self.output().connect()
        self.cursor = self.connection.cursor()
        # Build a list of properties from the dataset fields
        self.record_properties = dataset_get_properties(self.module_name)
        # Get all foreign keys
        self.foreign_keys = dataset_get_foreign_keys(self.module_name)
        # Get current specimen record count
        self.milestones = get_milestones(self.cursor)

    def create_table(self, connection):
        """
        Create table for the ke emu module
        This is an override of luigi.CopyToTable.create_table() to handle using
        Column objects, not tuples
        """

        # Build a string of column definitions
        coldefs = ','.join(
            '{name} {type}'.format(name=col.field_name, type=col.field_type) for col in self.columns
        )
        query = "CREATE TABLE {table} ({coldefs})".format(table=self.table, coldefs=coldefs)
        connection.cursor().execute(query)
        connection.commit()

    @property
    def sql(self):
        """
        SQL for insert / updates
        Tries inserting, and on conflict performs update with modified date
        :return: SQL
        """
        # Get any extra fields defined in the base module class
        # This uses a set comprehension converted into a list for deduping
        extra_fields = list({col.field_name for col in self.columns if col not in KeemuBaseTask.columns})
        insert_fields = ['irn', 'guid', 'properties', 'import_date'] + extra_fields
        update_fields = ['properties', 'import_date'] + extra_fields
        sql = """
            INSERT INTO {table_name} ({insert_fields}, created) VALUES ({insert_fields_placeholders}, NOW())
            ON CONFLICT (irn)
            DO UPDATE SET ({update_fields}, modified) = ({update_fields_placeholders}, NOW()) WHERE {table_name}.irn = %(irn)s RETURNING modified
        """.format(
            table_name=self.table,
            insert_fields=','.join(insert_fields),
            insert_fields_placeholders=','.join(map(lambda field: "%({0})s".format(field), insert_fields)),
            update_fields=','.join(update_fields),
            update_fields_placeholders=','.join(map(lambda field: "%({0})s".format(field), update_fields)),
        )
        return sql

    def requires(self):
        return KeemuFileTask(
            file_name='{module_name}.export'.format(module_name=self.module_name),
            date=self.date
        )

    def ensure_table(self):
        connection = self.output().connect()
        if not db_table_exists(self.table, connection):
            self.create_table(self.connection)
        # Create any foreign key tables
        if self.foreign_keys:
            for fk in self.foreign_keys:
                fk.create_table(self.connection)

    def _apply_filters(self, record):
        """
        Apply all the filters to determine if a record should be imported
        Return True if it can be; return False if it should be skipped
        @return: boolean
        """
        for record_filter in self.record_filters:
            if not record_filter.apply(record):
                return False
        return True

    def milestone_check(self, record_dict):
        """
        Checks to see if this record reaches any of the defined milestones
        :param record_dict: the current record
        """
        for m in self.milestones:
            m.check(record_dict)

    def run(self):
        # Ensure table exists
        self.ensure_table()
        start_time = time.time()

        for record in self.records():
            self.insert_count += 1
            record_dict = self._record_to_dict(record)
            self.milestone_check(record_dict)

            # Cast all dict objects to PGJson
            for key in record_dict.keys():
                if type(record_dict[key]) is dict:
                    record_dict[key] = PGJson(record_dict[key])

            # Insert the record
            self.cursor.execute(self.sql, record_dict)
            # The SQL upsert statement uses INSERT ON CONFLICT UPDATE...
            # Which always returns INSERT 1 in status message
            # So the UPDATE statement uses RETURNING modified
            # If we have a modified date, this is an update; otherwise an insert
            is_update = self.cursor.fetchone()[0] is not None
            # If we have foreign keys for this module, see if a foreign key
            # has been defined - and if it has insert it
            if self.foreign_keys:
                for fk in self.foreign_keys:
                    # If this is an update (has modified date), delete all
                    # foreign key relations before reinserting
                    # Prevents relationships data from getting stale
                    if is_update:
                        fk.delete(self.cursor, record)
                    # If this record has a relationship, insert it
                    rel_irn = getattr(record, fk.field_name, None)
                    if rel_irn:
                        fk.insert(self.cursor, record, rel_irn)

        # mark as complete in same transaction
        self.output().touch(self.connection)
        # Create indexes now - slightly speeds up the process if it happens afterwards
        self.ensure_indexes()
        # And commit everything
        self.connection.commit()
        logger.info('Inserted %d %s records in %d seconds', self.insert_count, self.table, time.time() - start_time)

    @property
    def file_input(self):
        """
        Helper to get reference to file input - allows ecatalogue to override
        @return: input file ref
        """
        return self.input()

    def records(self):
        for record in Parser(self.file_input.path):
            # Iterate record counter, even if it gets filtered out
            # makes debugging a bit simpler, as you can limit and test filters
            self.record_count += 1
            if self._apply_filters(record):
                yield record
            # Record is being filtered out
            # Before we continue, if the record has been marked as
            #  not web publishable, we try and delete it
            elif not self._is_web_publishable(record):
                self.delete_record(record)

            if self.limit and self.record_count >= self.limit:
                break

            if self.record_count % 1000 == 0:
                logger.debug('Record count: %d', self.record_count)

    def delete_record(self, record):
        """
        Mark a record as deleted
        :return: None
        """
        db_delete_record(self.table, record.irn, self.cursor)

    def ensure_indexes(self):
        for column in self.columns:
            if column.indexed:
                index_type = column.get_index_type()
                db_create_index(self.table, column.field_name, index_type, self.connection)

    @staticmethod
    def _is_web_publishable(record):
        """
        Evaluate whether a record is importable
        At the very least a record will need AdmPublishWebNoPasswordFlag set to Y,
        Additional models will extend this to provide additional filters
        :param record:
        :return: boolean - false if not importable
        """
        return record.AdmPublishWebNoPasswordFlag.lower() != 'n'

    def _record_to_dict(self, record):
        """
        Convert record object to a dict
        :param record:
        :return:
        """

        record_dict = {
            'irn': record.irn,
            'guid': record.AdmGUIDPreferredValue,
            'properties': self._record_map_fields(record, self.record_properties),
            'import_date': int(self.date)
        }
        # Loop through columns, adding any extra fields
        # These need to be set even if null as they are part of the SQL statement
        for column in self.columns:
            if column.ke_field_name:
                value = None
                # Ke field name can be a list - if it is loop through all the values
                if isinstance(column.ke_field_name, list):
                    for fn in column.ke_field_name:
                        value = getattr(record, fn, None)
                        # Once we have a value, break out of the loop
                        if value:
                            break
                else:
                    value = getattr(record, column.ke_field_name, None)

                record_dict[column.field_name] = value

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
        return {f.field_alias: f.get_value(record) for f in fields if f.has_value(record)}
