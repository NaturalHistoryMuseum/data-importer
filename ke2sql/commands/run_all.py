#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '31/03/2017'.
"""


import click
import luigi
from ke2sql.commands.helpers import get_unprocessed_export_dates
from ke2sql.tasks.__main__ import MainTask


@click.command()
@click.option('--local-scheduler',  default=False, help='Whether to use the luigi local scheduler.', is_flag=True)
@click.option('--limit', default=None, help='Number of records to process.', type=click.INT)
def run_all(local_scheduler, limit):
    """
    Run tasks for a single date
    Can either be a data passed in as a param, or it no param
    The oldest export file will be used
    :param local_scheduler:
    :param limit:
    :return: None
    """
    export_dates = get_unprocessed_export_dates()
    for index, export_date in enumerate(export_dates):
        params = {
            'date': export_date,
            'limit': limit
        }
        # Only refresh the view if this is the last export date being processed
        refresh_view = (index + 1) == len(export_dates)
        if refresh_view:
            params['refresh_view'] = True

        luigi.build([MainTask(**params)], local_scheduler=local_scheduler)

if __name__ == "__main__":
    run_all()
