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
from ke2psql.model.keemu import IndexLotModel, TaxonomyModel, MultimediaModel, SpecimenModel
import unittest

class TestIndexLotTask(BaseTask, CatalogueTask):
    module = 'ecatalogue'

class IndexLotTest(unittest.TestCase, BaseTest):

    file_name = 'indexlot.export'

    def setUp(self):

        # Need to create a taxonomy record to test against
        taxonomy = TaxonomyModel(irn=100, scientific_name='Dummy 1')
        self.session.merge(taxonomy)
        taxonomy = TaxonomyModel(irn=101, scientific_name='Dummy 2')
        self.session.merge(taxonomy)
        multimedia = MultimediaModel(irn=100)
        self.session.merge(multimedia)
        multimedia = MultimediaModel(irn=101)
        self.session.merge(multimedia)
        assoc_record = SpecimenModel(irn=100)
        self.session.merge(assoc_record)
        assoc_record = SpecimenModel(irn=101)
        self.session.merge(assoc_record)
        self.session.commit()

    task = TestIndexLotTask
    model = IndexLotModel

    # def test_deleted_material_detail(self):
    #     self._test_relationship_delete('material_detail')

    def test_material_detail_values(self):
        self._test_relationship_values('material_detail', 2)

    def test_multimedia_exists(self):
        self._test_relationship_exists('multimedia')

    def test_multimedia_delete(self):
        self._test_relationship_delete('multimedia')

    def test_associated_record_exists(self):
        self._test_relationship_exists('associated_record')

    def test_associated_record_delete(self):
        self._test_relationship_delete('associated_record')

    # Run all again as an update
    def test_update_deleted_material_detail(self):
        self.update()
        self._test_relationship_delete('material_detail')

    def test_update_material_detail_values(self):
        self.update()
        self._test_relationship_values('material_detail')

    def test_update_multimedia_exists(self):
        self.update()
        self._test_relationship_exists('multimedia')

    def test_update_multimedia_delete(self):
        self.update()
        self._test_relationship_delete('multimedia')

    def test_update_associated_record_exists(self):
        self.update()
        self._test_relationship_exists('associated_record')

    def test_update_associated_record_delete(self):
        self.update()
        self._test_relationship_delete('associated_record')

if __name__ == '__main__':
    unittest.main()


