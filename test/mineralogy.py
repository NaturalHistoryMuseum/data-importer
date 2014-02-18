#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import sys
import os
from catalogue import TestCatalogueTask
from ke2psql.model.keemu import MineralogySpecimenModel
import unittest
import specimen

class MineralogyTest(specimen.SpecimenTest):

    file_name = 'min.export'
    task = TestCatalogueTask
    model = MineralogySpecimenModel

    def test_mineralogy_data(self):

        self.create()
        obj = self.query().one()
        self.assertEquals(obj.host_rock, 'A12')
        self.assertEquals(obj.identification_as_registered, 'A13')
        self.assertEquals(obj.identification_other, 'A14')
        self.assertEquals(obj.identification_variety, 'A15')
        self.assertEquals(obj.commodity, 'A16')
        self.assertEquals(obj.deposit_type, 'A17')
        self.assertEquals(obj.occurrence, 'A18')
        self.assertEquals(obj.texture, 'A19')
        self.delete()

    def test_mineralogy_update(self):
        self.update()
        self.create()
        obj = self.query().one()
        self.assertEquals(obj.host_rock, 'B12')
        self.assertEquals(obj.identification_as_registered, 'B13')
        self.assertEquals(obj.identification_other, 'B14')
        self.assertEquals(obj.identification_variety, 'B15')
        self.assertEquals(obj.commodity, 'B16')
        self.assertEquals(obj.deposit_type, 'B17')
        self.assertEquals(obj.occurrence, 'B18')
        self.assertEquals(obj.texture, 'B19')
        self.delete()

    def test_age(self):
        self.create()
        obj = self.query().one()
        # Should be two objects
        self.assertEqual(len(obj.mineralogical_age), 2)
        self.assertEquals(obj.mineralogical_age[0].age, 'AD1')
        self.assertEquals(obj.mineralogical_age[0].age_type, 'AT1')
        self.assertEquals(obj.mineralogical_age[1].age, 'AD2')
        self.assertEquals(obj.mineralogical_age[1].age_type, 'AT2')
        self.delete()

    def test_age_update(self):
        self.update()
        self.create()
        obj = self.query().one()
        # Should be no objects
        self.assertEqual(len(obj.mineralogical_age), 0)
        self.delete()

    def test_age_delete(self):
        self._test_relationship_delete('mineralogical_age', table='keemu.specimen_mineralogical_age', key='mineralogy_irn')

    def test_sex_stage(self):
        pass

    def test_sex_stage_update(self):
        pass

if __name__ == '__main__':
    unittest.main()


