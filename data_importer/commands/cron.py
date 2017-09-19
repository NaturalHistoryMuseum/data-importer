#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '31/03/2017'.
"""

import click
import time
import luigi

from data_importer.tasks.specimen import SpecimenDatasetTask
from data_importer.tasks.indexlot import IndexLotDatasetTask
from data_importer.tasks.artefact import ArtefactDatasetTask


@click.command()
@click.option('--local-scheduler', default=False, help='Whether to use the luigi local scheduler.', is_flag=True)
def run_cron(local_scheduler):
    """
    Run tasks on cron - gets the current date, and runs tasks for that date
    This should be used in conjunction with a cron task, that schedules
    this command to be run for every day a known export is produced
    if an export is missing or corrupt (zero bytes) the tasks themselves
    will raise an error
    :param local_scheduler:
    :return: None
    """
    # Get today's date, formatted as per keemu export files - 20170608
    params = {
        'date': int(time.strftime("%Y%m%d")),
        'limit': 10000
    }
    for task in [SpecimenDatasetTask, IndexLotDatasetTask, ArtefactDatasetTask]:
        luigi.build([task(**params)], local_scheduler=local_scheduler)


if __name__ == "__main__":
    run_cron()
