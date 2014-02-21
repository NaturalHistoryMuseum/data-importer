#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""


from ke2sql.model import meta
from ke2sql.model.log import Log as LogModel
from ke2sql.log import log
import unittest
import uuid


class LogTest(unittest.TestCase):

    session = meta.session

    def test_critical(self):

        # Generate a unique identifier for the error message
        id = uuid.uuid4().hex
        log.critical(id)
        # Check we have the identifier in the DB
        self.session.query(LogModel).filter(LogModel.msg == id).one()




if __name__ == '__main__':
    unittest.main()