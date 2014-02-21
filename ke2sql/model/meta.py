#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import sys
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import URL
from ke2sql import config

__all__ = ['engine', 'session']

db_settings = dict(config.items('database'))
# Do not use the schema in the URL connection
db_settings.pop('schema')

engine = create_engine(URL(**db_settings))
session = sessionmaker(bind=engine)()

