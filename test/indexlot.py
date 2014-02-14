#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import sys
import os
from ke2psql.tasks import CatalogueTask
from base import BaseTask, BaseTest
from ke2psql.model.keemu import IndexLotModel, TaxonomyModel
import unittest

class TestIndexLotTask(BaseTask, CatalogueTask):
    module = 'ecatalogue'
    file_name = 'indexlot.export'

class IndexLotTest(unittest.TestCase, BaseTest):

    def setUp(self):

        # Need to create a taxonomy record to test against
        taxonomy = TaxonomyModel(irn=100, scientific_name='Dummy')
        self.session.merge(taxonomy)
        self.session.commit()

    task = TestIndexLotTask
    model = IndexLotModel

    def test_deleted_material_detail(self):
        self._test_deleted_relationship('material_detail')

    def test_material_detail_values(self):
        self._test_relationship_values('material_detail')


if __name__ == '__main__':
    unittest.main()


