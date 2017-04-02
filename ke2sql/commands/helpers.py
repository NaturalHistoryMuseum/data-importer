#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '31/03/2017'.
"""

import os
import re
import glob
import psycopg2

from ke2sql.lib.config import Config


def get_file_export_dates():
    """
    Get a list of all file export dates
    :return:
    """
    full_export_date = Config.getint('keemu', 'full_export_date')
    export_dir = Config.get('keemu', 'export_dir')
    file_pattern = 'ecatalogue.export.*.gz'
    re_date = re.compile(r"([0-9]+)\.gz$")
    export_dates = []
    # Loop through all the files in the export directory,
    # Building a list of dates that need to br processed
    for fn in glob.glob(os.path.join(export_dir, file_pattern)):
        m = re_date.search(fn)
        file_date = int(m.group(1))
        if file_date >= full_export_date:
            export_dates.append(file_date)
    # Sort in reverse date order
    export_dates.sort()
    return export_dates


def get_file_import_marker_dates():
    """
    Get file import marker dates - denotes which file tasks have already run
    :return:
    """
    re_date = re.compile(r"_([0-9]+)_")
    connection = psycopg2.connect(
        host=Config.get('database', 'host'),
        port=Config.get('database', 'port'),
        database=Config.get('database', 'datastore_dbname'),
        user=Config.get('database', 'username'),
        password=Config.get('database', 'password')
    )
    cursor = connection.cursor()
    sql = """ SELECT update_id
              FROM table_updates
          """
    cursor.execute(sql)
    # Parse the marker dates from each of the rows
    # FIXME: Check all tasks have run?
    return {int(re_date.search(r[0]).group(1)) for r in cursor.fetchall()}


def get_unprocessed_export_dates():
    """
    Return a list of all unprocessed export files
    :return:
    """
    marker_dates = get_file_import_marker_dates()
    unprocessed_dates = []
    for export_date in get_file_export_dates():
        if export_date not in marker_dates:
            unprocessed_dates.append(export_date)
    return unprocessed_dates


def get_oldest_unprocessed_export_date():
    """
    Get the oldest unprocessed date that hasn't been run yet
    Just
    :return:
    """
    return get_unprocessed_export_dates()[0]
