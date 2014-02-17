#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import sys
import os
from ke2psql.tasks import TaxonomyTask
from base import BaseTask, BaseTest
from ke2psql.model.keemu import TaxonomyModel
import unittest

class TestTaxonomyTask(BaseTask, TaxonomyTask):
    module = 'etaxonomy'

class TaxonomyTest(unittest.TestCase, BaseTest):
    task = TestTaxonomyTask
    model = TaxonomyModel

    def test_data(self):
        self.create()
        obj = self.query().one()
        self.assertEquals(obj.currently_accepted_name, True)
        self.assertEquals(obj.scientific_name_author, 'A1')
        self.assertEquals(obj.scientific_name_author_year, 'A2')
        self.assertEquals(obj.taxonomic_class, 'A3')
        self.assertEquals(obj.family, 'A5')
        self.assertEquals(obj.genus, 'A6')
        self.assertEquals(obj.kingdom, 'A7')
        self.assertEquals(obj.order, 'A8')
        self.assertEquals(obj.phylum, 'A9')
        self.assertEquals(obj.rank, 'A10')
        self.assertEquals(obj.scientific_name, 'A11')
        self.assertEquals(obj.species, 'A12')
        self.assertEquals(obj.subfamily, 'A13')
        self.assertEquals(obj.subgenus, 'A14')
        self.assertEquals(obj.suborder, 'A15')
        self.assertEquals(obj.subspecies, 'A16')
        self.assertEquals(obj.superfamily, 'A17')
        self.assertEquals(obj.validity, 'A18')
        self.delete()

    def test_update(self):
        self.update()
        self.create()
        obj = self.query().one()
        self.assertEquals(obj.currently_accepted_name, True)
        self.assertEquals(obj.scientific_name_author, 'B1')
        self.assertEquals(obj.scientific_name_author_year, 'B2')
        self.assertEquals(obj.taxonomic_class, 'B3')
        self.assertEquals(obj.family, 'B5')
        self.assertEquals(obj.genus, 'B6')
        self.assertEquals(obj.kingdom, 'B7')
        self.assertEquals(obj.order, 'B8')
        self.assertEquals(obj.phylum, 'B9')
        self.assertEquals(obj.rank, 'B10')
        self.assertEquals(obj.scientific_name, 'B11')
        self.assertEquals(obj.species, 'B12')
        self.assertEquals(obj.subfamily, 'B13')
        self.assertEquals(obj.subgenus, 'B14')
        self.assertEquals(obj.suborder, 'B15')
        self.assertEquals(obj.subspecies, 'B16')
        self.assertEquals(obj.superfamily, 'B17')
        self.assertEquals(obj.validity, 'B18')
        self.delete()

if __name__ == '__main__':
    unittest.main()