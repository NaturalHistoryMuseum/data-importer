import sys
import os
from ke2psql.tasks import CatalogueTask
from base import BaseTask, BaseTest
from catalogue import CatalogueTest, TestCatalogueTask
from indexlot import IndexLotTest
from mineralogy import MineralogyTest
from sites import SitesTest
from collectionevent import CollectionEventTest
from ke2psql.model.keemu import *
import unittest

class TempTest(CollectionEventTest):



    def test_part(self):
        # self.update()

        self.create()
        obj = self.query().one()

        print 'self.create()'
        print 'obj = self.query().one()'

        data = {}

        for field, value in obj:

            if field in ['irn', '_created', '_modified'] or isinstance(value, list):
                continue

            data[int(value[1:])] = 'self.assertEquals(obj.%s, \'%s\')' % (field, value)

        for key in sorted(sorted(data)):
            print data[key]

        print 'self.create()'


def export_data():

    model = CollectionEventModel()

    fields = []
    x = 1
    aliases = model.get_aliases()
    for alias in sorted(aliases):

        if alias in ['AdmDateInserted', 'AdmDateModified', 'ColDepartment']:
            continue

        if aliases[alias] not in fields:
            print '%s:1=B%s' % (alias, x)
            x += 1

        fields.append(aliases[alias])






if __name__ == '__main__':
    unittest.main()
    # export_data()