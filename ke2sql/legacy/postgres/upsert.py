#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '03/03/2017'.
"""

from psycopg2.extras import Json as PGJson
from luigi.contrib.postgres import CopyToTable as LuigiCopyToTable


class PostgresUpsertMixin(LuigiCopyToTable):
    """
    Extends CopyToTable to write directly to database using Upsert statements
    """
    def __init__(self, *args, **kwargs):
        # Initiate a DB connection
        super(PostgresUpsertMixin, self).__init__(*args, **kwargs)
        self.connection = self.output().connect()
        self.cursor = self.connection.cursor()
        # Quick look up list of integer fields
        self._int_fields = [col_name for col_name, col_def in self.get_column_types() if 'INTEGER' in col_def]

    @property
    def sql(self):
        """
        SQL for insert / updates
        Tries inserting, and on conflict performs update with modified date
        :return: SQL
        """
        extra_fields = list(set([f[1] for f in self._extra_field_mappings]))
        insert_fields = ['irn', 'properties'] + extra_fields
        update_fields = ['properties'] + extra_fields
        return """
            INSERT INTO {table_name} ({insert_fields}, created) VALUES ({insert_fields_placeholders}, NOW())
            ON CONFLICT (irn)
            DO UPDATE SET ({update_fields}, modified) = ({update_fields_placeholders}, NOW()) WHERE {table_name}.irn = %(irn)s
        """.format(
            table_name=self.table,
            insert_fields=','.join(insert_fields),
            insert_fields_placeholders=','.join(map(lambda field: "%({0})s".format(field), insert_fields)),
            update_fields=','.join(update_fields),
            update_fields_placeholders=','.join(map(lambda field: "%({0})s".format(field), update_fields)),
        )

    def ensure_table(self):
        if not self.table_exists(self.connection):
            self.create_table(self.connection)

    def run(self):
        # Ensure table exists
        self.ensure_table()
        # Loop through all the records, executing SQL
        for record in self.records():
            # psycopg2 encode dicts to Json
            for key in record.keys():
                if type(record[key]) is dict:
                    record[key] = PGJson(record[key])
                # If this is a list of integer fields, we need to manually
                # map the values to int as psycopg2 doesn't transform arrays
                elif type(record[key]) is list and key in self._int_fields:
                    record[key] = list(map(int, record[key]))
            self.cursor.execute(self.sql, record)
        # mark as complete in same transaction
        self.output().touch(self.connection)
        self.connection.commit()

    def delete_record(self, record):
        """
        Mark a record as deleted
        :return: None
        """
        sql = "UPDATE {table_name} SET (deleted) = (NOW()) WHERE {table_name}.irn = %(irn)s".format(
            table_name=self.table,
        )
        self.connection.execute(sql, irn=record.irn)
