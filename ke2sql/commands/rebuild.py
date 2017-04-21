#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '31/03/2017'.
"""


import click
import psycopg2
import logging

from ke2sql.lib.config import Config
from ke2sql.lib.helpers import list_all_modules
from ke2sql.commands.helpers import get_unprocessed_export_dates
from ke2sql.commands.helpers import run_tasks
from ke2sql.lib.db import db_table_exists


logger = logging.getLogger('luigi-interface')


@click.command()
@click.option('--local-scheduler',  default=False, help='Whether to use the luigi local scheduler.', is_flag=True)
def rebuild(local_scheduler):
    """
    Rebuild the dataset from scratch, reprocessing every export file
    :param local_scheduler:
    :return: None
    """

    connection = psycopg2.connect(
        host=Config.get('database', 'host'),
        port=Config.get('database', 'port'),
        database=Config.get('database', 'datastore_dbname'),
        user=Config.get('database', 'username'),
        password=Config.get('database', 'password')
    )
    cursor = connection.cursor()

    # Delete file markers
    if db_table_exists('table_updates', connection):
        cursor.execute('DELETE FROM table_updates')
    # Delete all info in the module tables
    for module_name in list_all_modules():
        if db_table_exists(module_name, connection):
            logger.info('Deleting existing records from %s', module_name)
            cursor.execute('DELETE FROM "{module_name}"'.format(
                module_name=module_name
            ))
    connection.commit()
    connection.close()
    export_dates = get_unprocessed_export_dates()
    run_tasks(export_dates, local_scheduler)

if __name__ == "__main__":
    rebuild()
