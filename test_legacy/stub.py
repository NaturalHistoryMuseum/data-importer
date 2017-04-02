#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""


from ke2sql.model.keemu import *
import unittest
from base import  BaseTest
from catalogue import TestCatalogueTask
from sqlalchemy.orm.exc import NoResultFound

class StubTest(unittest.TestCase, BaseTest):

    file_name = 'stub.export'
    task = TestCatalogueTask
    model = MammalGroupPartModel

    def test_associated_record(self):
        # NB: If this ever fails, try putting self.delete() here and rerunning
        self.create()
        obj = self.query().one()
        # Make sure we have a StubModel(200)
        self.assertIsInstance(obj.associated_record[0], StubModel)
        self.session.query(StubModel).filter(self.model.irn == 200)
        self.delete()

    def test_parent(self):
        self.create()
        obj = self.query().one()
        # Make sure we have a StubModel(200)
        self.assertIsInstance(obj.parent, StubModel)
        self.session.query(StubModel).filter(self.model.irn == 201)
        self.delete()

    def test_update(self):
        """
        The update file has code to update both stubs
        """
        self.update()
        self.create()
        obj = self.query().one()
        # Check the subs have been transformed into correct models
        self.session.query(IndexLotModel).filter(IndexLotModel.irn == 200).one()
        self.session.query(SpecimenModel).filter(SpecimenModel.irn == 201).one()
        # # Delete the objects
        self.delete()

    def delete(self):

        for irn in [200, 201]:
            try:
                stub = self.session.query(CatalogueModel).filter(CatalogueModel.irn == irn).one()
            except NoResultFound:
                pass
            else:
                self.session.delete(stub)

        obj = self.query().one()
        # Delete the obj
        self.session.delete(obj)
        self.session.commit()

    def test_not_null(self):
        pass

    def test_updated_date(self):
        """
        Override updated date - we don't update irn=1, just the stubs
        """
        pass


if __name__ == '__main__':
    unittest.main()