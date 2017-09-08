#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '22/03/2017'.
"""

import psycopg2
from data_importer.lib.db import db_table_exists


class Field(object):
    """
    Field definition
    :param module_name: keemu module
    :param field_name: keemu field name
    :param field_alias: the field name in the dataset
    """

    def __init__(self, module_name, field_name, field_alias):
        self.module_name = module_name
        self.field_name = field_name
        self.field_alias = field_alias


class ForeignKeyField(object):
    """
    Foreign Key field
    :param module_name:
    :param join_module:
    :param field_name: KE EMu field name
    """

    def __init__(self, module_name, join_module, field_name):
        self.module_name = module_name
        self.join_module = join_module
        self.field_name = field_name

    @property
    def table(self):
        """
        Table name is just module name, but is required by LuigiCopyToTable
        :return: string
        """
        return '_{}__{}'.format(self.module_name, self.join_module)

    @property
    def insert_sql(self):
        """
        SQL for inserting
        Uses SELECT...WHERE EXISTS to ensure the IRN exists in the join table
        """
        sql = """
          INSERT INTO {table_name}(irn, rel_irn)
          SELECT %(irn)s, %(rel_irn)s WHERE EXISTS(SELECT 1 FROM {join_module} where irn=%(rel_irn)s);
        """.format(
            table_name=self.table,
            join_module=self.join_module
        )
        return sql

    @property
    def delete_sql(self):
        sql = """
            DELETE FROM {table_name} WHERE irn = %(irn)s
        """.format(
            table_name=self.table,
        )
        return sql

    def create_table(self, connection):
        if not db_table_exists(self.table, connection):
            query = """
              CREATE TABLE {table} (
                irn int references {module_name}(irn),
                rel_irn int references {join_module}(irn)
              )
            """.format(
                table=self.table,
                module_name=self.module_name,
                join_module=self.join_module
            )
            connection.cursor().execute(query)
            # Create indexes - postgres does not index reference fields
            query = """
              CREATE INDEX ON {table} (irn, rel_irn)
            """.format(
                table=self.table,
            )
            connection.cursor().execute(query)

    def delete(self, cursor, record):
        cursor.execute(self.delete_sql, {'irn': record.irn})

    def insert(self, cursor, record, rel_irn):

        # We can get a list of IRNs, so convert to list so we can easily loop
        # Also, ensure all are integers
        irn_list = [int(rel_irn)] if not isinstance(rel_irn, list) else map(int, rel_irn)
        for irn in irn_list:
            cursor.execute(self.insert_sql, {
                'irn': record.irn,
                'rel_irn': irn
            })
