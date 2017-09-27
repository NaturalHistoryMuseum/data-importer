#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '31/03/2017'.

 python commands/run.py 20170906

"""

import click
import time
import luigi

from data_importer.tasks.specimen import SpecimenDatasetTask
from data_importer.tasks.indexlot import IndexLotDatasetTask
from data_importer.tasks.artefact import ArtefactDatasetTask


@click.command()
@click.argument('date')
@click.option('--local-scheduler', default=False, help='Whether to use the luigi local scheduler.', is_flag=True)
def run_cron(date, local_scheduler):
    """
    Helper command to run all three dataset tasks
    :param date: data of import to run.     
    :param local_scheduler:
    :return: None
    """
    for task in [SpecimenDatasetTask, IndexLotDatasetTask, ArtefactDatasetTask]:
        luigi.build([task(date=date)], local_scheduler=local_scheduler)


if __name__ == "__main__":
    run_cron()
