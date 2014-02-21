#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""


import luigi.postgres
from keparser import KEParser
from ke2sql.log import log
from ke2sql.model.meta import config
from ke2sql.model import meta
from ke2sql.model.keemu import *
from ke2sql.tasks.ke import KEFileTask, KEDataTask
from ke2sql.tasks import *
from sqlalchemy.orm.exc import NoResultFound
from datetime import datetime, timedelta

class DeleteTask(luigi.postgres.CopyToTable):

    date = luigi.DateParameter(default=None)
    module = 'eaudit'
    file_name = 'deleted-export'

    host = config.get('database', 'host', 'localhost')
    database = config.get('database', 'database')
    user = config.get('database', 'username')
    password = config.get('database', 'password')
    # No table; we're going to use SQLAlchemy
    table = None

    keemu_schema_file = config.get('keemu', 'schema')
    session = meta.session

    def requires(self):
        # TODO: Make dependent on catalogue task
        return KEFileTask(module=self.module, date=self.date, file_name=self.file_name)

    def run(self):

        # Need to load an SQLA model
        # So build a dict of all models keyed by KE EMu module
        models = {}

        for cls in KEDataTask.__subclasses__():
            models[cls.module] = cls.model_class if cls.model_class else CatalogueModel

        ke_data = KEParser(self.input().open('r'), schema_file=self.keemu_schema_file, input_file_path=self.input().path)

        for data in ke_data:
            module = data.get('AudTable')
            irn = data.get('AudKey')
            try:
                model = models[module]
            except KeyError:
                log.debug('Skipping eaudit record for %s' % module)
            else:

                try:

                    log.debug('Deleting record %s(%s)' % (model, irn))

                    # Load the object and then delete so we use the SQLA inheritance
                    obj = self.session.query(self.model).filter(self.model.irn == 1).one()
                    self.session.delete(obj)

                except NoResultFound:

                    # We cannot delete this record as it doesn't exist
                    # There are a lot of records being inserted and then deleted again
                    # So will never appear on the insert exports
                    date_inserted = datetime.strptime(data.get('AdmDateInserted'),"%Y-%m-%d")
                    date_deleted = datetime.strptime(data.get('AudDate'),"%Y-%m-%d")

                     # If date deleted is within 7 days of the insert date, do not flag an error
                    if date_deleted - timedelta(days=7) < date_inserted:
                        log.debug('Record %s(%s) not found for deletion, but within date threshold (inserted: %s deleted: %s)' % (model.__name__, irn, date_inserted, date_deleted))
                    else:
                        log.error('Record %s(%s) not found for deletion' % (model, irn))

        self.session.commit()

        # TODO: Mark this task as complete
        # self.output().touch()


