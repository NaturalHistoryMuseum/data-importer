#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'Ben Scott' on 2013-06-21.
"""

import logging
import traceback
from ke2sql.model.log import Log
from ke2sql.model import meta


class SQLAlchemyHandler(logging.Handler):
    # A very basic logger that commits a LogRecord to the SQL Db
    def emit(self, record):

        trace = None
        exc = record.__dict__['exc_info']

        if exc:
            trace = traceback.format_exc(exc)
        log = Log(
            logger=record.__dict__['name'],
            level=record.__dict__['levelname'],
            trace=trace,
            msg=record.__dict__['msg'],
            args=record.args
        )

        meta.session.add(log)
        meta.session.commit()