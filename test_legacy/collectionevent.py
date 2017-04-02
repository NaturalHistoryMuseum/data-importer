#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import sys
import os
from ke2sql.tasks import CollectionEventsTask
from base import BaseTask, BaseTest
from ke2sql.model.keemu import CollectionEventModel
import unittest

class CollectionEventTask(BaseTask, CollectionEventsTask):
    module = 'ecollectionevents'

class CollectionEventTest(unittest.TestCase, BaseTest):

    task = CollectionEventTask
    model = CollectionEventModel

    def test_data(self):
        self.create()
        obj = self.query().one()
        self.assertEquals(obj.vessel_name, 'A1')
        self.assertEquals(obj.vessel_type, 'A2')
        self.assertEquals(obj.collection_event_code, 'A3')
        self.assertEquals(obj.collection_method, 'A4')
        self.assertEquals(obj.date_collected_from, 'A5')
        self.assertEquals(obj.date_collected_to, 'A6')
        self.assertEquals(obj.time_collected_from, 'A7')
        self.assertEquals(obj.time_collected_to, 'A8')
        self.assertEquals(obj.depth_from_fathoms, 'A9')
        self.assertEquals(obj.depth_from_feet, 'A10')
        self.assertEquals(obj.depth_from_metres, 'A11')
        self.assertEquals(obj.depth_to_fathoms, 'A12')
        self.assertEquals(obj.depth_to_feet, 'A13')
        self.assertEquals(obj.depth_to_metres, 'A14')
        self.assertEquals(obj.expedition_name, 'A15')
        self.assertEquals(obj.expedition_start_date, 'A16')
        self.delete()

    def test_update(self):
        self.update()
        self.create()
        obj = self.query().one()
        self.assertEquals(obj.vessel_name, 'B1')
        self.assertEquals(obj.vessel_type, 'B2')
        self.assertEquals(obj.collection_event_code, 'B3')
        self.assertEquals(obj.collection_method, 'B4')
        self.assertEquals(obj.date_collected_from, 'B5')
        self.assertEquals(obj.date_collected_to, 'B6')
        self.assertEquals(obj.time_collected_from, 'B7')
        self.assertEquals(obj.time_collected_to, 'B8')
        self.assertEquals(obj.depth_from_fathoms, 'B9')
        self.assertEquals(obj.depth_from_feet, 'B10')
        self.assertEquals(obj.depth_from_metres, 'B11')
        self.assertEquals(obj.depth_to_fathoms, 'B12')
        self.assertEquals(obj.depth_to_feet, 'B13')
        self.assertEquals(obj.depth_to_metres, 'B14')
        self.assertEquals(obj.expedition_name, 'B15')
        self.assertEquals(obj.expedition_start_date, 'B16')
        self.delete()

if __name__ == '__main__':
    unittest.main()


