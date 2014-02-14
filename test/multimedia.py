#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import sys
import os
from ke2psql.tasks import MultimediaTask
from base import BaseTask, BaseTest
from ke2psql.model.keemu import MultimediaModel
import unittest

class TestMultimediaTask(BaseTask, MultimediaTask):
    module = 'emultimedia'

class MultimediaTest(unittest.TestCase, BaseTest):

    task = TestMultimediaTask
    model = MultimediaModel

if __name__ == '__main__':
    unittest.main()


