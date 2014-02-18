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
from ke2psql.model.keemu import *
import unittest
import datetime

class TestCatalogueTask(BaseTask, CatalogueTask):
    module = 'ecatalogue'

class CatalogueTest(unittest.TestCase, BaseTest):

    file_name = 'export'

    def setUp(self):

        # Set up all the records we need for taxonomy & relationships
        self.session.merge(TaxonomyModel(irn=100, scientific_name='Dummy 1'))
        self.session.merge(TaxonomyModel(irn=101, scientific_name='Dummy 2'))
        self.session.merge(SiteModel(irn=100))
        self.session.merge(SiteModel(irn=101))
        self.session.merge(CollectionEventModel(irn=100))
        self.session.merge(CollectionEventModel(irn=101))
        self.session.merge(MultimediaModel(irn=100))
        self.session.merge(MultimediaModel(irn=101))
        self.session.merge(SpecimenModel(irn=100))
        self.session.merge(SpecimenModel(irn=101))
        self.session.commit()

    task = TestCatalogueTask
    model = CatalogueModel

    def test_catalogue_data(self):
        """
        Test the basic
        """
        self.create()
        obj = self.query().one()
        self.assertEquals(obj.ke_date_modified.strftime('%Y-%m-%d'), '1999-01-31')
        self.assertEquals(obj.ke_date_inserted.strftime('%Y-%m-%d'), '1999-01-31')
        self.delete()

    def test_catalogue_data_update(self):
        """
        Test the updated data
        """
        self.update()
        self.create()
        obj = self.query().one()
        self.assertEquals(obj.ke_date_modified.strftime('%Y-%m-%d'), '2000-01-31')
        self.assertEquals(obj.ke_date_inserted.strftime('%Y-%m-%d'), '2000-01-31')
        self.delete()

    def test_type(self):
        self.create()
        obj = self.query().one()
        self.assertIsInstance(obj, self.model)
        self.delete()

    def test_multimedia_exists(self):
        self._test_relationship_exists('multimedia')

    def test_multimedia_delete(self):
        self._test_relationship_delete('multimedia')

    def test_associated_record_exists(self):
        self._test_relationship_exists('associated_record')

    def test_associated_record_delete(self):
        self._test_relationship_delete('associated_record')

    def test_update_multimedia_not_exists(self):
        self.update()
        self._test_relationship_not_exists('multimedia')

    def test_update_multimedia_delete(self):
        self.update()
        self._test_relationship_delete('multimedia')

    def test_update_associated_record_not_exists(self):
        self.update()
        self._test_relationship_not_exists('associated_record')

    def test_update_associated_record_delete(self):
        self.update()
        self._test_relationship_delete('associated_record')


if __name__ == '__main__':
    unittest.main()


