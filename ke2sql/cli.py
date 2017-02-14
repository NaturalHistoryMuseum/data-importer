#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '14/02/2017'.
"""

import click

from ke2sql import db


@click.command()
def init():
    """Initiation script - creating schema and tables"""
    db.init()
    click.echo('Schema and tables created')
