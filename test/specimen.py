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

    def test_specimen_data(self):
        self.create()
        obj = self.query().one()
        self.assertEquals(obj.date_identified_year, 2000)
        self.assertEquals(obj.date_identified_month, 1)
        self.assertEquals(obj.date_identified_day, 1)
        self.assertEquals(obj.catalogue_number, 'A1')
        self.assertEquals(obj.kind_of_collection, 'A2')
        self.assertEquals(obj.specimen_unit, 'A3')
        self.assertEquals(obj.preservation, 'A4')
        self.assertEquals(obj.collection_sub_department, 'A5')
        self.assertEquals(obj.type_status, 'A6')
        self.assertEquals(obj.date_catalogued, 'A7')
        self.assertEquals(obj.verbatim_label_data, 'A8')
        self.assertEquals(obj.donor_name, 'A9')
        self.assertEquals(obj.registration_code, 'A10')
        self.assertEquals(obj.curation_unit, 'A11')
        self.assertEquals(obj.site_irn, 100)
        self.assertEquals(obj.collection_event_irn, 100)
        self.assertEquals(obj.collection_department, 'CD1')
        self.create()

    def test_specimen_update(self):
        self.update()
        self.create()
        obj = self.query().one()
        self.assertEquals(obj.date_identified_year, 1999)
        self.assertEquals(obj.date_identified_month, 1)
        self.assertEquals(obj.date_identified_day, 1)
        self.assertEquals(obj.catalogue_number, 'B1')
        self.assertEquals(obj.kind_of_collection, 'B2')
        self.assertEquals(obj.specimen_unit, 'B3')
        self.assertEquals(obj.preservation, 'B4')
        self.assertEquals(obj.collection_sub_department, 'B5')
        self.assertEquals(obj.type_status, 'B6')
        self.assertEquals(obj.date_catalogued, 'B7')
        self.assertEquals(obj.verbatim_label_data, 'B8')
        self.assertEquals(obj.donor_name, 'B9')
        self.assertEquals(obj.registration_code, 'B10')
        self.assertEquals(obj.curation_unit, 'B11')
        self.assertEquals(obj.collection_department, 'CD2')
        self.create()

    def test_determination(self):

        self.create()
        # Load the obj from the database
        obj = self.query().one()
        self.assertEquals(obj.determination[0].irn, 100)
        self.assertEquals(obj.determination[1].irn, 101)
        self.assertEquals(obj.specimen_taxonomy[1].filed_as, True)
        self.delete()

    def test_determination_update(self):

        # Update file has no determination - check they are removed
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
        self.assertEquals(obj.other_numbers[0].kind, 'O1')
        self.assertEquals(obj.other_numbers[0].value, '1')
        self.assertEquals(obj.other_numbers[1].kind, 'O2')
        self.assertEquals(obj.other_numbers[1].value, '2')
        self.delete()

    def test_other_numbers_update(self):
        """
        Other numbers are deleted
        """
        self.update()
        self.create()
        obj = self.query().one()
        # Should be zero objects - other numbers are deleted in update
        self.assertEqual(len(obj.other_numbers), 0)

        self.delete()

    def test_other_numbers_delete(self):
        self._test_relationship_delete('other_numbers')

    def test_sex_stage(self):
        self.create()
        obj = self.query().one()

        # Should be two objects
        self.assertEqual(len(obj.sex_stage), 2)
        self.assertEquals(obj.sex_stage[0].count, 1)
        self.assertEquals(obj.sex_stage[0].sex, 'S1')
        self.assertEquals(obj.sex_stage[0].stage, '')
        self.assertEquals(obj.sex_stage[1].count, 2)
        self.assertEquals(obj.sex_stage[1].sex, 'S2')
        self.assertEquals(obj.sex_stage[1].stage, 'S3')
        self.delete()

    def test_sex_stage_update(self):
        self.update()
        self.create()
        obj = self.query().one()
        # Should be one object
        self.assertEqual(len(obj.sex_stage), 1)
        self.assertEquals(obj.sex_stage[0].count, 3)
        self.assertEquals(obj.sex_stage[0].sex, 'T1')
        self.assertEquals(obj.sex_stage[0].stage, 'T2')
        self.delete()

    def test_other_numbers_delete(self):
        self._test_relationship_delete('sex_stage', table='keemu.specimen_sex_stage', key='specimen_irn')


if __name__ == '__main__':
    unittest.main()


