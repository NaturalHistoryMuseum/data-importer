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

class SpecimenTest(CatalogueTest):

    file_name = 'specimen.export'
    task = TestCatalogueTask
    model = SpecimenModel

    def test_data(self):
        # TODO
        pass

    def test_determination(self):

        self.create()
        # Load the obj from the database
        obj = self.query().one()

        self.assertEquals(obj.determination[0].irn, 100)
        self.assertEquals(obj.determination[1].irn, 101)
        self.assertEquals(obj.specimen_taxonomy[1].filed_as, True)
        self.delete()

    def test_determination_update(self):

        # Update file has not determination - check they are removed
        self.update()
        self.create()
        obj = self.query().one()
        self.assertEquals(obj.determination, [])
        self.assertEquals(obj.specimen_taxonomy, [])
        self.delete()

    def test_determination_delete(self):
        self._test_relationship_delete('determination', table='keemu.determination', key='specimen_irn')

    def test_other_numbers(self):
        self.create()
        obj = self.query().one()

        # Should be two objects
        self.assertEqual(len(obj.other_numbers), 2)
        self.assertEquals(obj.other_numbers[0].kind, 'T1')
        self.assertEquals(obj.other_numbers[0].value, '1')
        self.assertEquals(obj.other_numbers[1].kind, 'T2')
        self.assertEquals(obj.other_numbers[1].value, '2')
        self.delete()

    def test_other_numbers_update(self):
        self.update()
        self.create()
        obj = self.query().one()
        # Should be one object
        self.assertEqual(len(obj.other_numbers), 1)
        self.assertEquals(obj.other_numbers[0].kind, 'T3')
        self.assertEquals(obj.other_numbers[0].value, '3')
        self.delete()

    def test_other_numbers_delete(self):
        self._test_relationship_delete('other_numbers')

    def test_sex_stage(self):
        self.create()
        obj = self.query().one()

        # Should be two objects
        self.assertEqual(len(obj.sex_stage), 2)
        self.assertEquals(obj.sex_stage[0].count, 1)
        self.assertEquals(obj.sex_stage[0].sex, 'Male')
        self.assertEquals(obj.sex_stage[0].stage, '')
        self.assertEquals(obj.sex_stage[1].count, 2)
        self.assertEquals(obj.sex_stage[1].sex, 'Female')
        self.assertEquals(obj.sex_stage[1].stage, 'Adult')
        self.delete()

    def test_sex_stage_update(self):
        self.update()
        self.create()
        obj = self.query().one()
        # Should be one object
        self.assertEqual(len(obj.sex_stage), 1)
        self.assertEquals(obj.sex_stage[0].count, 3)
        self.assertEquals(obj.sex_stage[0].sex, '')
        self.assertEquals(obj.sex_stage[0].stage, 'Juvenile')
        self.delete()

    def test_other_numbers_delete(self):
        self._test_relationship_delete('sex_stage', table='keemu.specimen_sex_stage', key='specimen_irn')


if __name__ == '__main__':
    unittest.main()


