#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '31/03/2017'.
"""

import click
from ke2sql.commands.helpers import get_oldest_unprocessed_export_date
from ke2sql.commands.helpers import run_tasks


@click.command()
@click.option('--local-scheduler',  default=False, help='Whether to use the luigi local scheduler.', is_flag=True)
@click.option('--limit', default=None, help='Number of records to process.', type=click.INT)
def run_one(local_scheduler, limit):
    """
    Run tasks for a single date
    :param local_scheduler:
    :param limit:
    :return: None
    """

    date = get_oldest_unprocessed_export_date()
    if not date:
        print('No more files to process')
        return

    run_tasks([date], local_scheduler, limit)

if __name__ == "__main__":
    run_one()
