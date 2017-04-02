#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '31/03/2017'.
"""

import click
import luigi
from ke2sql.commands.helpers import get_oldest_unprocessed_export_date
from ke2sql.tasks.__main__ import MainTask


@click.command()
@click.option('--local-scheduler',  default=False, help='Whether to use the luigi local scheduler.', is_flag=True)
@click.option('--limit', default=None, help='Number of records to process.', type=click.INT)
@click.option('--date', default=None, help='Export date to process.', type=click.INT)
def run_one(local_scheduler, limit, date):
    """
    Run tasks for a single date
    Can either be a data passed in as a param, or it no param
    The oldest export file will be used
    :param local_scheduler:
    :param limit:
    :param date:
    :return: None
    """
    # If we haven't specified a
    if not date:
        date = get_oldest_unprocessed_export_date()

    params = [
        '--date', str(date),
        '--limit', str(limit)
    ]
    luigi.run(params, main_task_cls=MainTask, local_scheduler=local_scheduler)

if __name__ == "__main__":
    run_one()
