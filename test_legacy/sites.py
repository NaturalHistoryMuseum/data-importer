#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import sys
import os
from ke2sql.tasks import SitesTask
from base import BaseTask, BaseTest
from ke2sql.model.keemu import SiteModel
import unittest

class TestSitesTask(BaseTask, SitesTask):
    module = 'esites'


class SitesTest(unittest.TestCase, BaseTest):
    task = TestSitesTask
    model = SiteModel

    def test_data(self):
        self.create()
        obj = self.query().one()
        self.assertEquals(obj.water_body, 'A1')
        self.assertEquals(obj.river_basin, 'A2')
        self.assertEquals(obj.vice_county, 'A3')
        self.assertEquals(obj.mineral_complex, 'A4')
        self.assertEquals(obj.mining_district, 'A5')
        self.assertEquals(obj.geology_region, 'A6')
        self.assertEquals(obj.mine, 'A7')
        self.assertEquals(obj.tectonic_province, 'A8')
        self.assertEquals(obj.geodetic_datum, 'A9')
        self.assertEquals(obj.georef_method, 'A10')
        self.assertEquals(obj.latitude, 'A11')
        self.assertEquals(obj.decimal_latitude, 'A12')
        self.assertEquals(obj.longitude, 'A13')
        self.assertEquals(obj.decimal_longitude, 'A14')
        self.assertEquals(obj.continent, 'A15')
        self.assertEquals(obj.country, 'A16')
        self.assertEquals(obj.county, 'A17')
        self.assertEquals(obj.island_group, 'A18')
        self.assertEquals(obj.island, 'A19')
        self.assertEquals(obj.nearest_named_place, 'A20')
        self.assertEquals(obj.locality, 'A21')
        self.assertEquals(obj.ocean, 'A22')
        self.assertEquals(obj.state_province, 'A23')
        self.assertEquals(obj.town, 'A24')
        self.assertEquals(obj.minimum_elevation_in_meters, 'A25')
        self.assertEquals(obj.maximum_elevation_in_meters, 'A26')
        self.assertEquals(obj.minimum_depth_in_meters, 'A27')
        self.assertEquals(obj.maximum_depth_in_meters, 'A28')
        self.assertEquals(obj.parish, 'A29')
        self.delete()

    def test_update(self):
        self.update()
        self.create()
        obj = self.query().one()
        self.assertEquals(obj.water_body, 'B1')
        self.assertEquals(obj.river_basin, 'B2')
        self.assertEquals(obj.vice_county, 'B3')
        self.assertEquals(obj.mineral_complex, 'B4')
        self.assertEquals(obj.mining_district, 'B5')
        self.assertEquals(obj.geology_region, 'B6')
        self.assertEquals(obj.mine, 'B7')
        self.assertEquals(obj.tectonic_province, 'B8')
        self.assertEquals(obj.geodetic_datum, 'B9')
        self.assertEquals(obj.georef_method, 'B10')
        self.assertEquals(obj.latitude, 'B11')
        self.assertEquals(obj.decimal_latitude, 'B12')
        self.assertEquals(obj.longitude, 'B13')
        self.assertEquals(obj.decimal_longitude, 'B14')
        self.assertEquals(obj.continent, 'B15')
        self.assertEquals(obj.country, 'B16')
        self.assertEquals(obj.county, 'B17')
        self.assertEquals(obj.island_group, 'B18')
        self.assertEquals(obj.island, 'B19')
        self.assertEquals(obj.nearest_named_place, 'B20')
        self.assertEquals(obj.locality, 'B21')
        self.assertEquals(obj.ocean, 'B22')
        self.assertEquals(obj.state_province, 'B23')
        self.assertEquals(obj.town, 'B24')
        self.assertEquals(obj.minimum_elevation_in_meters, 'B25')
        self.assertEquals(obj.maximum_elevation_in_meters, 'B26')
        self.assertEquals(obj.minimum_depth_in_meters, 'B27')
        self.assertEquals(obj.maximum_depth_in_meters, 'B28')
        self.assertEquals(obj.parish, 'B29')
        self.delete()

if __name__ == '__main__':
    unittest.main()