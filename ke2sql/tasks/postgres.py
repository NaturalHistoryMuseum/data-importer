#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '03/03/2017'.
"""

import json
from collections import OrderedDict
from psycopg2.extras import Json as PGJson
from luigi.contrib.postgres import CopyToTable as LuigiCopyToTable


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
        return """
                INSERT INTO {table_name} (irn, properties, metadata, created)
                  VALUES (%(irn)s, %(properties)s, %(metadata)s, NOW())
                ON CONFLICT (irn)
                DO UPDATE SET (properties, metadata, modified) = (%(properties)s, %(metadata)s, NOW()) WHERE {table_name}.irn = %(irn)s
        """.format(
            table_name=self.table
        )

    def ensure_table(self):
        if not self.table_exists():
            self.create_table(self.connection)

    def table_exists(self):
        self.cursor.execute("SELECT 1 FROM information_schema.tables WHERE table_name=%s", (self.table,))
        return bool(self.cursor.rowcount)

    def run(self):
        # Ensure table exists
        self.ensure_table()
        # Loop through all the records, executing SQL
        for record in self.records():
            # psycopg2 encode dicts to Json
            for key in record.keys():
                if type(record[key]) is dict:
                    record[key] = PGJson(record[key])
            self.cursor.execute(self.sql, record)

        self.connection.commit()

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
        # Populate row using the same order as columns
        ordered_cols = OrderedDict(self.columns).keys()
        # Loop through records, building and yielding rows (lists)
        for record in self.records():
            row = []
            record['created'] = 'NOW()'
            for col in ordered_cols:
                value = record.get(col, None)
                if type(value) is dict:
                    value = json.dumps(value)
                row.append(value)
            yield row

    def delete_record(self, record):
        # No need to delete on copy
        pass