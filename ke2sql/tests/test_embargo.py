#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '31/03/2017'.
"""

import unittest


from ke2sql.tests import BaseTestCase


class TestEmbargo(BaseTestCase):

    def test_embargoed_specimen_exists_in_ecatalogue(self):
        irn = 13
        self.assertRecordExists('ecatalogue', irn)

    def test_embargoed_specimen_is_not_in_dataset(self):
        irn = 13
        self.assertDatasetRecordDoesNotExist('specimen', irn)

    def test_embargoed_by_extension_specimen_is_imported_but_not_released_in_dataset(self):
        irn = 14
        self.assertRecordExists('ecatalogue', irn)
        self.assertDatasetRecordDoesNotExist('specimen', irn)

    def test_expired_embargoed_specimen_is_released_in_dataset(self):
        irn = 15
        self.assertRecordExists('ecatalogue', irn)
        self.assertDatasetRecordExists('specimen', irn)

    def test_embargoed_multimedia_record_has_embargoed_date(self):
        record = self._get_record('emultimedia', irn=3)
        self.assertIsNotNone(record['embargo_date'])

    def test_specimen_record_does_not_used_embargoed_multimedia(self):
        record = self._get_dataset_record('specimen', irn=20)
        self.assertIsNone(record['multimedia'])

if __name__ == '__main__':
    unittest.main()
