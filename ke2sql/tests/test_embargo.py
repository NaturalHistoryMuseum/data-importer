#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '31/03/2017'.
"""

import os
import luigi
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

    def test_past_embargoed_specimen_is_released_in_dataset(self):
        irn = 15
        self.assertRecordExists('ecatalogue', irn)
        self.assertDatasetRecordExists('specimen', irn)

if __name__ == '__main__':
    unittest.main()
