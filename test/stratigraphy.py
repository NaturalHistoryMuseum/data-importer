#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import sys
import os
from ke2psql.tasks import StratigraphyTask
from base import BaseTask, BaseTest
from ke2psql.model.keemu import StratigraphyModel
import unittest

class TestStratigraphyTask(BaseTask, StratigraphyTask):
    module = 'enhmstratigraphy'


class StratigraphyTest(unittest.TestCase, BaseTest):

    task = TestStratigraphyTask
    model = StratigraphyModel

if __name__ == '__main__':
    unittest.main()


