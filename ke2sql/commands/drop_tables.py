#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '31/03/2017'.
"""


import click
import psycopg2
import logging
from prompter import yesno

from ke2sql.lib.config import Config
from ke2sql.lib.helpers import list_all_modules
from ke2sql.commands.helpers import get_unprocessed_export_dates
from ke2sql.commands.helpers import run_tasks
from ke2sql.lib.db import db_drop_table


logger = logging.getLogger('luigi-interface')


@click.command()
def drop_tables():
    """
    Drop all tables
    :return: None
    """

    connection = psycopg2.connect(
        host=Config.get('database', 'host'),
        port=Config.get('database', 'port'),
        database=Config.get('database', 'datastore_dbname'),
        user=Config.get('database', 'username'),
        password=Config.get('database', 'password')
    )

    if yesno('Your are dropping all tables - all data will be deleted. Are you sure you want to continue?'):
        db_drop_table('table_updates', connection)
        # Delete all info in the module tables
        for module_name in list_all_modules():
            db_drop_table(module_name, connection)
        connection.commit()
        connection.close()

if __name__ == "__main__":
    drop_tables()
