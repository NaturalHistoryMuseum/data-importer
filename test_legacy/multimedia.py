#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import sys
import os
from ke2sql.tasks import MultimediaTask
from base import BaseTask, BaseTest
from ke2sql.model.keemu import MultimediaModel
import unittest

class TestMultimediaTask(BaseTask, MultimediaTask):
    module = 'emultimedia'

class MultimediaTest(unittest.TestCase, BaseTest):

    task = TestMultimediaTask
    model = MultimediaModel

    def test_data(self):
        self.create()
        obj = self.query().one()
        self.assertEquals(obj.title, 'A1')
        self.assertEquals(obj.mime_type, 'image')
        self.assertEquals(obj.mime_format, 'A2')
        self.delete()

    def test_update(self):
        self.update()
        self.create()
        obj = self.query().one()
        self.assertEquals(obj.title, 'B1')
        self.assertEquals(obj.mime_type, 'image')
        self.assertEquals(obj.mime_format, 'B2')
        self.delete()

if __name__ == '__main__':
    unittest.main()


