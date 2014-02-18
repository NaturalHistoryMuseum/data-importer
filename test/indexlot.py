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
from catalogue import CatalogueTest, TestCatalogueTask
from ke2psql.model.keemu import IndexLotModel, TaxonomyModel, MultimediaModel, SpecimenModel
import unittest


class IndexLotTest(CatalogueTest):

    file_name = 'indexlot.export'

    task = TestCatalogueTask
    model = IndexLotModel

    def test_indexlot_data(self):
        self.create()
        obj = self.query().one()
        self.assertEquals(obj.type, 'indexlot')
        self.assertEquals(obj.kind_of_material, 'A1')
        self.assertEquals(obj.kind_of_media, 'A2')
        self.assertEquals(obj.material, True)
        self.assertEquals(obj.media, True)
        self.assertEquals(obj.is_type, True)
        self.assertEquals(obj.taxonomy_irn, 100)
        self.assertIsInstance(obj.determination, TaxonomyModel)
        self.delete()

    def test_indexlot_update(self):
        self.update()
        self.create()
        obj = self.query().one()
        self.assertEquals(obj.type, 'indexlot')
        self.assertEquals(obj.kind_of_material, 'B1')
        self.assertEquals(obj.kind_of_media, 'B2')
        self.assertEquals(obj.material, False)
        self.assertEquals(obj.media, False)
        self.assertEquals(obj.is_type, False)
        self.assertEquals(obj.taxonomy_irn, 101)
        self.assertIsInstance(obj.determination, TaxonomyModel)
        self.delete()

    def test_material_detail(self):
        self.create()
        obj = self.query().one()

        # Should be two objects
        self.assertEqual(len(obj.material_detail), 2)
        self.assertEquals(obj.material_detail[0].count, 1)
        self.assertEquals(obj.material_detail[0].sex, 'A6')
        self.assertEquals(obj.material_detail[0].stage, None)
        self.assertEquals(obj.material_detail[0].types, 'A8')
        self.assertEquals(obj.material_detail[0].primary_type_number, '1')
        self.assertEquals(obj.material_detail[1].count, 2)
        self.assertEquals(obj.material_detail[1].sex, 'A7')
        self.assertEquals(obj.material_detail[1].stage, 'A10')
        self.assertEquals(obj.material_detail[1].types, 'A9')
        self.assertEquals(obj.material_detail[1].primary_type_number, None)
        self.delete()

    def test_material_detail_delete(self):
        self._test_relationship_delete('material_detail')

    def test_material_detail_update(self):
        self.update()
        self.create()
        obj = self.query().one()

        # Should be one object
        self.assertEqual(len(obj.material_detail), 1)
        self.assertEquals(obj.material_detail[0].count, 3)
        self.assertEquals(obj.material_detail[0].sex, None)
        self.assertEquals(obj.material_detail[0].stage, 'B3')
        self.assertEquals(obj.material_detail[0].types, None)
        self.assertEquals(obj.material_detail[0].primary_type_number, '1.1')
        self.delete()

if __name__ == '__main__':
    unittest.main()


