#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '21/03/2017'.
"""


def db_view_exists(view_name, connection):
    cursor = connection.cursor()
    cursor.execute("select exists(select * from pg_matviews where matviewname=%s)", (view_name,))
    return cursor.fetchone()[0]


def db_create_index(table_name, field_name, index_type, connection):
    cursor = connection.cursor()
    query = "CREATE INDEX ON {table} USING {index_type} ({field_name})".format(
        table=table_name,
        index_type=index_type,
        field_name=field_name
    )
    cursor.execute(query)


def db_table_exists(table_name, connection):
    """
    Check if a table exists
    :param table_name:
    :param connection:
    :return:
    """
    cursor = connection.cursor()
    cursor.execute("select exists(select * from information_schema.tables where table_name=%s)", (table_name,))
    return cursor.fetchone()[0]


def db_drop_table(table_name, connection):
    """
    Drop the table
    :param table_name
    :param connection:
    :return:
    """
    query = "DROP TABLE IF EXISTS {table} CASCADE".format(table=table_name)
    connection.cursor().execute(query)
    connection.commit()


def db_delete_record(table_name, irn, cursor):
    """
    Mark a record as deleted
    :param table_name
    :param irn: record irn
    :param cursor:
    :return:
    """
    sql = "UPDATE {table_name} SET (deleted) = (NOW()) WHERE {table_name}.irn = %s".format(
        table_name=table_name,
    )
    cursor.execute(sql, (irn,))
