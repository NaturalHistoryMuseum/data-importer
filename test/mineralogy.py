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
from catalogue import CatalogueTest, TestCatalogueTask
from ke2psql.model.keemu import MineralogySpecimenModel, SiteModel, CollectionEventModel
import unittest

class MineralogyTest(CatalogueTest):

    file_name = 'min.export'
    task = TestCatalogueTask
    model = MineralogySpecimenModel

    def test_data(self):

        self.create()
        obj = self.query().one()
        self.assertEquals(obj.collection_department, 'V1')
        self.assertEquals(obj.type, 'mineralogy')
        self.assertEquals(obj.ke_date_modified.strftime('%Y-%m-%d'), '1999-01-31')
        self.assertEquals(obj.ke_date_inserted.strftime('%Y-%m-%d'), '1999-01-31')
        self.assertEquals(obj.catalogue_number, 'V2')
        self.assertEquals(obj.collection_sub_department, 'V7')
        self.assertEquals(obj.commodity, 'V24')
        self.assertEquals(obj.curation_unit, 'V35')
        self.assertEquals(obj.date_catalogued, 'V14')
        self.assertEquals(obj.date_identified_day, 15)
        self.assertEquals(obj.date_identified_month, 01)
        self.assertEquals(obj.date_identified_year, 1999)
        self.assertEquals(obj.date_registered, '2010')
        self.assertEquals(obj.deposit_type, 'V25')
        self.assertEquals(obj.donor_name, 'V28')
        self.assertEquals(obj.host_rock, 'V20')
        self.assertEquals(obj.identification_as_registered, 'V21')
        self.assertEquals(obj.identification_other, 'V22')
        self.assertEquals(obj.identification_variety, 'V23')
        self.assertEquals(obj.kind_of_collection, 'V3')
        self.assertEquals(obj.occurrence, 'V26')
        self.assertEquals(obj.preservation, 'V5')
        self.assertEquals(obj.registration_code, 'V34')
        self.assertEquals(obj.specimen_unit, 'V4')
        self.assertEquals(obj.texture, 'V27')
        self.assertEquals(obj.type_status, 'V11')
        self.assertEquals(obj.collection_event_irn, 100)
        self.assertEquals(obj.site_irn, 100)
        self.assertIsInstance(obj.site, SiteModel)
        self.assertIsInstance(obj.collection_event, CollectionEventModel)
        self.delete()

    def test_update(self):
        self.update()
        self.create()
        obj = self.query().one()
        self.assertEquals(obj.collection_department, 'V1')
        self.assertEquals(obj.type, 'mineralogy')
        self.assertEquals(obj.ke_date_modified.strftime('%Y-%m-%d'), '2000-01-31')
        self.assertEquals(obj.ke_date_inserted.strftime('%Y-%m-%d'), '2000-01-31')
        self.assertEquals(obj.catalogue_number, 'U2')
        self.assertEquals(obj.collection_sub_department, 'U7')
        self.assertEquals(obj.commodity, 'U24')
        self.assertEquals(obj.curation_unit, 'U35')
        self.assertEquals(obj.date_catalogued, 'U14')
        self.assertEquals(obj.date_identified_day, 01)
        self.assertEquals(obj.date_identified_month, 10)
        self.assertEquals(obj.date_identified_year, 2000)
        self.assertEquals(obj.date_registered, '2010')
        self.assertEquals(obj.deposit_type, 'U25')
        self.assertEquals(obj.donor_name, 'U28')
        self.assertEquals(obj.host_rock, 'U20')
        self.assertEquals(obj.identification_as_registered, 'U21')
        self.assertEquals(obj.identification_other, 'U22')
        self.assertEquals(obj.identification_variety, 'U23')
        self.assertEquals(obj.kind_of_collection, 'U3')
        self.assertEquals(obj.occurrence, 'U26')
        self.assertEquals(obj.preservation, 'U5')
        self.assertEquals(obj.registration_code, 'U34')
        self.assertEquals(obj.specimen_unit, 'U4')
        self.assertEquals(obj.texture, 'U27')
        self.assertEquals(obj.type_status, 'U11')
        self.assertEquals(obj.collection_event_irn, 101)
        self.assertEquals(obj.site_irn, 101)
        self.assertIsInstance(obj.site, SiteModel)
        self.assertIsInstance(obj.collection_event, CollectionEventModel)
        self.delete()

    def test_age(self):
        self.create()
        obj = self.query().one()
        # Should be two objects
        self.assertEqual(len(obj.mineralogical_age), 2)
        self.assertEquals(obj.mineralogical_age[0].age, 'Pliocene')
        self.assertEquals(obj.mineralogical_age[0].age_type, 'Mineralisation Age')
        self.assertEquals(obj.mineralogical_age[1].age, 'Miocene')
        self.assertEquals(obj.mineralogical_age[1].age_type, 'Host Age')
        self.delete()

    def test_age_update(self):
        self.update()
        self.create()
        obj = self.query().one()
        # Should be no objects
        self.assertEqual(len(obj.mineralogical_age), 0)
        self.delete()

    def test_age_delete(self):
        self._test_relationship_delete('mineralogical_age', table='keemu.specimen_mineralogical_age', key='mineralogy_irn')

if __name__ == '__main__':
    unittest.main()


