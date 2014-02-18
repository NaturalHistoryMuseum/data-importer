#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import sys
import os

from catalogue import TestCatalogueTask
from ke2psql.model.keemu import PalaeontologySpecimenModel, StratigraphyModel
import unittest
import specimen

class PalaeoTest(specimen.SpecimenTest):

    file_name = 'paleo.export'
    task = TestCatalogueTask
    model = PalaeontologySpecimenModel

    def setUp(self):

        # Create stratigraphy models
        self.session.merge(StratigraphyModel(irn=100))
        self.session.merge(StratigraphyModel(irn=101))
        self.session.commit()
        super(PalaeoTest, self).setUp()

    def test_paleo_data(self):

        self.create()
        # Load the obj from the database
        obj = self.query().one()
        self.assertEquals(obj.catalogue_description, 'A12')
        self.assertEquals(obj.stratigraphy_irn, 100)
        self.assertIsInstance(obj.stratigraphy, StratigraphyModel)
        self.delete()

    def test_paleo_update(self):
        self.update()
        self.create()
        # Load the obj from the database
        obj = self.query().one()
        self.assertEquals(obj.catalogue_description, 'B12')
        self.assertEquals(obj.stratigraphy_irn, 101)
        self.assertIsInstance(obj.stratigraphy, StratigraphyModel)
        self.delete()

    def test_sex_stage(self):
        self.create()
        # Load the obj from the database
        obj = self.query().one()
        self.assertEquals(obj.sex_stage, [])
        self.delete()

    def test_sex_stage_update(self):
        self.update()
        self.create()
        # Load the obj from the database
        obj = self.query().one()
        self.assertEquals(obj.sex_stage, [])
        self.delete()

    def test_determination(self):
        self.create()
        # Load the obj from the database
        obj = self.query().one()
        self.assertEquals(obj.determination, [])
        self.delete()

if __name__ == '__main__':
    unittest.main()


