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
from ke2psql.model.keemu import BirdGroupPartModel, SpecimenModel
import unittest

class PartTest(CatalogueTest):

    file_name = 'part.export'
    task = TestCatalogueTask
    model = BirdGroupPartModel

    def setUp(self):

        # Create a specimen model to use as a parent
        self.session.merge(SpecimenModel(irn=100))
        self.session.commit()

    def test_data(self):

        self.create()
        # Load the obj from the database
        obj = self.query().one()

        self.assertIsInstance(obj.parent, SpecimenModel)
        self.assertEquals(obj.parent.irn, 100)

        # Make sure it's got the specimen data too
        self.assertEquals(obj.curation_unit, 'Bird')
        self.assertEquals(obj.part_type, 'Leg')

        self.delete()

    def test_update(self):

        self.update()

        self.create()
        # Load the obj from the database
        obj = self.query().one()
        # In the update we've remove the rel
        self.assertIsNone(obj.parent)
        self.delete()


    def test_parent(self):
        """
        Test the link back to the part from the parent record
        """
        self.update()
        parent = self.session.query(SpecimenModel).filter(SpecimenModel.irn == 100).one()
        self.assertIsInstance(parent.part_record[0], self.model)
        self.assertEquals(parent.part_record[0].irn, 1)
        self.delete()


if __name__ == '__main__':
    unittest.main()


