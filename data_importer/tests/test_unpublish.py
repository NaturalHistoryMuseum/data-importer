#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '31/03/2017'.
"""

import unittest
from data_importer.tests import BaseTestCase


class TestUnpublish(BaseTestCase):

    def test_unpublished_specimen_previously_published_is_marked_deleted(self):
        self.assertRecordIsDeleted('ecatalogue', 19)

    def test_unpublished_specimen_previously_published_is_not_released(self):
        self.assertDatasetRecordDoesNotExist('specimen', 19)

if __name__ == '__main__':
    unittest.main()
