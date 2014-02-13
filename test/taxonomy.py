#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import sys
import os
from ke2psql.tasks import TaxonomyTask
from base import BaseTask, BaseTest
from ke2psql.model.keemu import TaxonomyModel
import unittest

class TestTaxonomyTask(BaseTask, TaxonomyTask):
    module = 'etaxonomy'

class TaxonomyTest(unittest.TestCase, BaseTest):
    task = TestTaxonomyTask
    model = TaxonomyModel


if __name__ == '__main__':
    unittest.main()