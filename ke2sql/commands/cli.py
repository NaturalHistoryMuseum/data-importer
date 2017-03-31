#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import os
import re
import click
import glob
import luigi
import psycopg2

from ke2sql.lib.config import Config
from ke2sql.lib.db import db_get_table_update_markers
from ke2sql.tasks.delete import DeleteTask
from ke2sql.tasks.specimen import SpecimenDatasetTask
from ke2sql.tasks.indexlot import IndexLotDatasetTask
from ke2sql.tasks.artefact import ArtefactDatasetTask


def get_file_export_dates():
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
    export_dates.sort()
    return export_dates


def get_file_import_marker_dates():
    """
    Get file import marker dates
    :return:
    """
    marker_dates = []
    connection = psycopg2.connect(
        host=Config.get('database', 'host'),
        port=Config.get('database', 'port'),
        database=Config.get('database', 'datastore_dbname'),
        user=Config.get('database', 'username'),
        password=Config.get('database', 'password')
    )
    update_markers = db_get_table_update_markers(connection)
    # TODO: Parsers for export date
    return marker_dates


@click.command()
@click.option('--local_scheduler',  default=False, help='Whether to use the luigi local scheduler.', is_flag=True)
@click.option('--limit', default=None, help='Number of records to process.', type=click.INT)
def run_import(local_scheduler, limit):
    full_export_date = Config.getint('keemu', 'full_export_date')
    # Get date of last run
    file_export_dates = get_file_export_dates()

    dataset_tasks = [
        ArtefactDatasetTask,
        IndexLotDatasetTask,
        SpecimenDatasetTask
    ]

    # Loop through all the dates
    for index, file_export_date in enumerate(file_export_dates):
        date_param = ['--date', str(file_export_date)]
        # Run delete if  if this isn't the full-export (no eaudit)
        # if file_export_date != full_export_date:
        #     luigi.run(date_param, main_task_cls=DeleteTask, local_scheduler=local_scheduler)

        # Extra params for dataset task
        dataset_task_params = date_param + [
            '--limit', str(limit)
        ]
        # If we're running multiple imports, only refresh views
        # for the last date in the set
        refresh_view = (index + 1) == len(file_export_dates)
        if refresh_view:
            dataset_task_params.append('--refresh-view')

        luigi.run(dataset_task_params, main_task_cls=IndexLotDatasetTask, local_scheduler=local_scheduler)
        # luigi.run(dataset_task_params, main_task_cls=SpecimenDatasetTask, local_scheduler=local_scheduler)

        # print(dataset_task_params)
        #
        # for task_cls in get_dataset_tasks():
        #     luigi.run(dataset_task_params, main_task_cls=task_cls, local_scheduler=local_scheduler)

        #     params = {
        #         'date': file_export_dates,
        #         'refresh_view': refresh_view,
        #         'limit': limit
        #     }
        #     # TODO: Run the task
        #     # TODO: Add rebuild views option
        #     # TODO: Change date to more recent one
        #     task(date=file_export_dates, limit=limit, refresh_view=refresh_view)


if __name__ == "__main__":
    run_import()