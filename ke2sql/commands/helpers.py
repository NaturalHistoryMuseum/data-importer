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
from ke2sql.lib.helpers import get_file_export_dates, get_dataset_tasks

def get_file_import_markers():
    """
    Get file import marker dates - denotes which file tasks have already run
    :return:
    """
    re_date = re.compile(r"([a-z]+)_([0-9]+)", re.IGNORECASE)
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
    markers = {}
    try:
        cursor.execute(sql)
    except psycopg2.ProgrammingError:
        # table_updates does not exist
        pass
    else:
        # Parse the marker dates from each of the rows

        for r in cursor.fetchall():
            m = re_date.search(r[0])
            task_name = m.group(1)
            marker_date = int(m.group(2))
            markers.setdefault(marker_date, []).append(task_name)

    return markers


def get_unprocessed_export_dates():
    """
    Return a list of all unprocessed export files
    Needs to have all dataset tasks run for a date to be seen as complete
    :return:
    """
    dataset_task_names = [cls.__name__ for cls in get_dataset_tasks()]
    markers = get_file_import_markers()

    unprocessed_dates = []
    # Loop through all the file export dates
    for export_date in get_file_export_dates():
        try:
            # Do we have markers for this date?
            markers_for_date = markers[export_date]
        except KeyError:
            unprocessed_dates.append(export_date)
        else:
            # We do have markers - check if all dataset tasks for date
            # Have been processed correctly
            if not set(dataset_task_names).issubset(set(markers_for_date)):
                unprocessed_dates.append(export_date)

    return unprocessed_dates


def get_oldest_unprocessed_export_date():
    """
    Get the oldest unprocessed date that hasn't been run yet
    Just
    :return:
    """
    try:
        return get_unprocessed_export_dates()[0]
    except IndexError:
        return None
