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
from ke2psql.model.keemu import PalaeontologySpecimenModel, StratigraphyModel
import unittest

class PalaeoTest(CatalogueTest):

    file_name = 'paleo.export'
    task = TestCatalogueTask
    model = PalaeontologySpecimenModel

    def setUp(self):

        # Create stratigraphy models
        self.session.merge(StratigraphyModel(irn=100))
        self.session.merge(StratigraphyModel(irn=101))
        self.session.commit()
        super(PalaeoTest, self).setUp()

    def test_data(self):

        self.create()
        # Load the obj from the database
        obj = self.query().one()
        self.assertEquals(obj.catalogue_description, 'Desc')
        self.assertEquals(obj.stratigraphy_irn, 100)
        self.assertIsInstance(obj.stratigraphy, StratigraphyModel)
        self.delete()

    def test_update(self):
        self.update()
        self.create()
        # Load the obj from the database
        obj = self.query().one()
        self.assertEquals(obj.catalogue_description, 'Desc2')
        self.assertEquals(obj.stratigraphy_irn, 101)
        self.assertIsInstance(obj.stratigraphy, StratigraphyModel)
        self.delete()

if __name__ == '__main__':
    unittest.main()


