#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '31/03/2017'.
"""

import unittest

from ke2sql.tests import BaseTestCase


class TestDelete(BaseTestCase):

    def test_deleted_specimen_is_removed(self):
        irn = 100
        self.assertRecordIsDeleted('ecatalogue', irn)
        self.assertDatasetRecordDoesNotExist('specimen', irn)

    def test_deleted_multimedia_is_removed(self):
        irn = 100
        self.assertRecordIsDeleted('emultimedia', irn)

    def test_deleted_multimedia_is_removed_from_specimen(self):
        irn = 16
        record = self._get_dataset_record('specimen', irn)
        # Check the specimen does not have multimedia record
        self.assertIsNone(record['associatedMedia'])

    def test_deleted_taxonomy_is_removed(self):
        irn = 100
        self.assertRecordIsDeleted('etaxonomy', irn)

    def test_deleted_taxonomy_is_removed_from_indexlot(self):
        irn = 18
        record = self._get_dataset_record('indexlot', irn)
        # Check the specimen does not have a scientific name
        # As the corresponding taxonomy record has been deleted
        self.assertIsNone(record['properties'].get('scientificName', None))


if __name__ == '__main__':
    unittest.main()
