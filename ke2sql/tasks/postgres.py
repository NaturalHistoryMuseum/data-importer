#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '03/03/2017'.
"""

from luigi.contrib.postgres import CopyToTable as LuigiCopyToTable

import json
import time
from abc import abstractproperty
from psycopg2.extras import Json as PGJson


class UpdateTable(LuigiCopyToTable):
    """
    Extending CopyToTable to write directly to database
    """
    def __init__(self, *args, **kwargs):
        # Initiate a DB connection
        super(UpdateTable, self).__init__(*args, **kwargs)
        self.connection = self.output().connect()
        self.cursor = self.connection.cursor()

    @property
    def sql(self):
        """
        SQL for insert / updates
        Tries inserting, and on conflict performs update with modified date
        :return: SQL
        """
        primary_key = 'irn'
        metadata_fields = set(['created', 'modified', 'deleted'])
        fields = set(dict(self.columns).keys())

        insert_fields = fields - metadata_fields
        # FIXME: What is the syntax for this??
        update_fields = fields - metadata_fields
        update_fields.remove(primary_key)

        return """
            INSERT INTO {table_name} ({insert_fields}, created) VALUES ({insert_fields_placeholders}, NOW())
            ON CONFLICT ({primary_key})
            DO UPDATE SET ({update_fields}, modified) = ({update_fields_placeholders}, NOW()) WHERE {table_name}.{primary_key} = %({primary_key})s
        """.format(
            table_name=self.table,
            insert_fields=', '.join(insert_fields),
            insert_fields_placeholders=', '.join(map(lambda field: "%({0})s".format(field), insert_fields)),
            primary_key=primary_key,
            update_fields=', '.join(update_fields),
            update_fields_placeholders=', '.join(map(lambda field: "%({0})s".format(field), update_fields)),
        )

    def run(self):

        for record in self.records():
            # psycopg2 encode Json
            record['properties'] = PGJson(record['properties'])
            self.cursor.execute(self.sql, record)

    def delete_record(self, record):
        """
        Marks a record as deleted
        :return: None
        """
        # print("DELETE")
        # self.connection.execute(self.model.delete_sql, irn=record.irn)
        pass


class CopyToTable(LuigiCopyToTable):
    """
    Extending CopyToTable with row mapper
    """
    def rows(self):
        """
        Implementation of CopyToTable.rows()
        :return:
        """
        # FIXME: Use a mapper here rather then yielding twice
        for record in self.records():
            row = (
                int(record['irn']),  # IRN
                int(time.time()),  # Date Created
                None,  # Date Updated
                None,  # Date Deleted
                json.dumps(record['properties']),  # Properties
            )
            yield row

    def delete_record(self, record):
        # No need to delete on copy
        pass