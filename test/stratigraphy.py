#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import sys
import os
from ke2sql.tasks import StratigraphyTask
from base import BaseTask, BaseTest
from ke2sql.model.keemu import StratigraphyModel, STRATIGRAPHIC_UNIT_TYPES
import unittest
import re

class TestStratigraphyTask(BaseTask, StratigraphyTask):
    module = 'enhmstratigraphy'

class StratigraphyTest(unittest.TestCase, BaseTest):

    task = TestStratigraphyTask
    model = StratigraphyModel

    def test_data(self):
        self.create()
        obj = self.query().one()
        self.assertEquals(obj.biozone, 'A1')
        self.assertEquals(obj.sedimentology, 'A2')
        self.assertEquals(obj.sedimentary_facies, 'A3')
        self.delete()

    def test_update(self):
        self.update()
        self.create()
        obj = self.query().one()
        self.assertEquals(obj.biozone, 'B1')
        self.assertEquals(obj.sedimentology, 'B2')
        self.assertEquals(obj.sedimentary_facies, 'B3')
        self.delete()

    def test_units(self):
        self.create()
        obj = self.query().one()

        # Count all variations of unit types - we should have all of them
        count = 0
        for group in STRATIGRAPHIC_UNIT_TYPES.values():
            for unit_type in group:
                for direction in ['To', 'From']:
                    count += 1

        self.assertEqual(len(obj.stratigraphic_unit), count)

        # Check we have the values correct (they were all generated to be type_direction: epoch_from
        for unit in obj.stratigraphic_unit:
            value = '{}_{}'.format(unit.stratigraphic_type, unit.direction)
            self.assertEquals(unit.unit.name, value)

    def test_units_update(self):
        self.update()

        # Count how many units are in the table before update
        count = self.session.scalar("SELECT COUNT(*) FROM keemu.stratigraphic_unit")
        self.create()
        after_update_count = self.session.scalar("SELECT COUNT(*) FROM keemu.stratigraphic_unit")

        # Should be the same even though values have changed: unit names stay once entered
        self.assertEquals(count, after_update_count)

        obj = self.query().one()

        # Count all variations of unit types - we should have all of them
        count = 0

        #  We've remove Biostratigraphy for testing
        del STRATIGRAPHIC_UNIT_TYPES['Biostratigraphy']

        for group in STRATIGRAPHIC_UNIT_TYPES.values():

            for unit_type in group:
                for direction in ['To', 'From']:
                    count += 1

        self.assertEqual(len(obj.stratigraphic_unit), count)

        # Check we have the values correct (they were all generated to be type_direction: epoch_from
        for unit in obj.stratigraphic_unit:
            value = '{}_{}_B'.format(unit.stratigraphic_type, unit.direction)
            self.assertEquals(unit.unit.name, value)

    def test_units_delete(self):
        self.create()
        count = self.session.scalar("SELECT COUNT(*) FROM keemu.stratigraphic_unit")
        self.delete()
        after_delete_count = self.session.scalar("SELECT COUNT(*) FROM keemu.stratigraphic_unit")

        # Should be the same even though record deleted: unit names stay once entered
        self.assertEquals(count, after_delete_count)

    def test_stratigraphic_unit_delete(self):
        """
        Check specimen related relationship is deleted
        """
        self._test_relationship_delete('stratigraphic_unit', table='keemu.stratigraphy_stratigraphic_unit', key='stratigraphy_irn')


if __name__ == '__main__':
    unittest.main()


