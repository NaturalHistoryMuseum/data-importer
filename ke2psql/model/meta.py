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
from ConfigParser import ConfigParser

config = ConfigParser()
config.read(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'client.cfg'))

__all__ = ['engine', 'session']

engine = create_engine(URL(**dict(config.items('database'))))
session = sessionmaker(bind=engine)()

