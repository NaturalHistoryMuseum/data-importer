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
from part import PartTest
from ke2psql.model.keemu import EggModel, SpecimenModel
import unittest

class EggTest(PartTest):

    file_name = 'egg.export'
    task = TestCatalogueTask
    model = EggModel

    def setUp(self):

        # Create a specimen model to use as a parent
        self.session.merge(SpecimenModel(irn=100))
        self.session.commit()

    def test_egg_data(self):

        self.create()
        # Load the obj from the database
        obj = self.query().one()

        self.assertIsInstance(obj.parent, SpecimenModel)
        self.assertEquals(obj.parent.irn, 100)

        # Make sure it's got the specimen data too
        self.assertEquals(obj.part_type, 'egg')
        self.assertEquals(obj.clutch_size, 1)
        self.assertEquals(obj.set_mark, 'A12')

        self.delete()

    def test_egg_update(self):

        self.update()
        self.create()
        # Load the obj from the database
        obj = self.query().one()
        # Make sure it's got the specimen data too
        self.assertEquals(obj.part_type, 'egg')
        self.assertEquals(obj.clutch_size, 2)
        self.assertEquals(obj.set_mark, 'B12')
        self.delete()

if __name__ == '__main__':
    unittest.main()


