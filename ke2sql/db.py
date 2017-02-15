#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '14/02/2017'.
"""

import sqlalchemy

from ke2sql.lib import config
from ke2sql.models.base import Base

engine = None


def get_engine():
    """
    Return the active database engine - or create a new one
    if does not exist
    :return: sqlalchemy db engine
    """
    global engine
    if not engine:
        cfg = config()
        engine = sqlalchemy.create_engine(cfg.get('postgres', 'url'))
    return engine


def init():
    """
    Initialise the DB
    :return:
    """
    Base.metadata.create_all(get_engine())


def get_connection():
    """
    Get DB engine
    :return:
    """
    return get_engine().connect()


def create_schema():
    # TODO: These should be in a separate, non-public schema
    pass