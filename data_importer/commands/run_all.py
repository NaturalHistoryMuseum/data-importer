#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '31/03/2017'.
"""


import click
from data_importer.commands.helpers import get_unprocessed_export_dates
from data_importer.commands.helpers import run_tasks


@click.command()
@click.option('--local-scheduler',  default=False, help='Whether to use the luigi local scheduler.', is_flag=True)
@click.option('--limit', default=None, help='Number of records to process.', type=click.INT)
def run_all(local_scheduler, limit):
    export_dates = get_unprocessed_export_dates()
    if export_dates:
        run_tasks(export_dates, local_scheduler, limit)
    else:
        print('No more files to process')

if __name__ == "__main__":
    run_all()
