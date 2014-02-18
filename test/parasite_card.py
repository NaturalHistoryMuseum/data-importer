#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import sys
import os
from catalogue import TestCatalogueTask
from ke2sql.model.keemu import ParasiteCardModel
import unittest
import specimen

class ParasiteCardTest(specimen.SpecimenTest):

    file_name = 'parasite_card.export'
    task = TestCatalogueTask
    model = ParasiteCardModel

    def test_data(self):

        self.create()
        # Load the obj from the database
        obj = self.query().one()
        self.assertEquals(obj.barcode, 'A12')
        self.delete()

    def test_data_update(self):
        self.update()
        self.create()
        # Load the obj from the database
        obj = self.query().one()
        self.assertEquals(obj.barcode, 'B12')
        self.delete()

    def test_host_parasite(self):

        self.create()
        # Load the obj from the database
        obj = self.query().one()

        self.assertEqual(len(obj.host_parasite), 2)

        self.assertEquals(obj.host_parasite_taxonomy[0].taxonomy_irn, 100)
        self.assertEquals(obj.host_parasite_taxonomy[0].parasite_host, 'parasite')
        self.assertEquals(obj.host_parasite_taxonomy[0].stage, 'A13')
        self.assertEquals(obj.host_parasite_taxonomy[1].taxonomy_irn, 101)
        self.assertEquals(obj.host_parasite_taxonomy[1].parasite_host, 'parasite')
        self.assertEquals(obj.host_parasite_taxonomy[1].stage, 'A14')
        self.delete()

    def test_host_parasite_update(self):

        self.update()
        self.create()

        obj = self.query().one()

        # Update uses both host and parasite (CardParasiteRef:1=101 CardHostRef:1=100) in *:1
        self.assertEqual(len(obj.host_parasite), 2)

        self.assertEquals(obj.host_parasite_taxonomy[0].taxonomy_irn, 100)
        self.assertEquals(obj.host_parasite_taxonomy[0].parasite_host, 'host')
        self.assertEquals(obj.host_parasite_taxonomy[0].stage, 'B13')

        self.assertEquals(obj.host_parasite_taxonomy[1].taxonomy_irn, 101)
        self.assertEquals(obj.host_parasite_taxonomy[1].parasite_host, 'parasite')
        self.assertEquals(obj.host_parasite_taxonomy[1].stage, 'B13')
        self.delete()

    def test_delete(self):
        self._test_relationship_delete('host_parasite', table='keemu.host_parasite', key='parasite_card_irn')

if __name__ == '__main__':
    unittest.main()


