#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '31/03/2017'.
"""

import unittest
from ke2sql.tests import BaseTestCase


class TestFilters(BaseTestCase):
    """
    Tests for checking imports work correctly
    """
    def test_specimen_record_with_invalid_collection_department_is_not_imported(self):
        irn = 4
        self.assertRecordDoesNotExist('ecatalogue', irn)
        self.assertDatasetRecordDoesNotExist('specimen', irn)

    def test_specimen_record_with_invalid_record_type_is_not_imported(self):
        irn = 5
        self.assertRecordDoesNotExist('ecatalogue', irn)
        self.assertDatasetRecordDoesNotExist('specimen', irn)

    def test_specimen_record_with_stub_record_status_is_not_imported(self):
        irn = 6
        self.assertRecordDoesNotExist('ecatalogue', irn)
        self.assertDatasetRecordDoesNotExist('specimen', irn)

    def test_specimen_record_with_missing_guid_is_not_imported(self):
        irn = 7
        self.assertRecordDoesNotExist('ecatalogue', irn)
        self.assertDatasetRecordDoesNotExist('specimen', irn)

    def test_non_web_publishable_specimen_record_is_not_imported(self):
        irn = 8
        self.assertRecordDoesNotExist('ecatalogue', irn)
        self.assertDatasetRecordDoesNotExist('specimen', irn)

    def test_indexlot_record_with_missing_guid_is_not_imported(self):
        irn = 9
        self.assertRecordDoesNotExist('ecatalogue', irn)
        self.assertDatasetRecordDoesNotExist('indexlot', irn)

    def test_indexlot_record_with_inactive_record_status_is_not_imported(self):
        irn = 10
        self.assertRecordDoesNotExist('ecatalogue', irn)
        self.assertDatasetRecordDoesNotExist('indexlot', irn)

    def test_non_web_publishable_indexlot_record_is_not_imported(self):
        irn = 11
        self.assertRecordDoesNotExist('ecatalogue', irn)
        self.assertDatasetRecordDoesNotExist('indexlot', irn)

    def test_non_web_publishable_multimedia_record_is_not_imported(self):
        irn = 2
        self.assertRecordDoesNotExist('emultimedia', irn)

    def test_non_web_publishable_taxonomy_record_is_not_imported(self):
        irn = 2
        self.assertRecordDoesNotExist('etaxonomy', irn)

if __name__ == '__main__':
    unittest.main()
