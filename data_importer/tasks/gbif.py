#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '08/09/2017'.
"""

import os
import csv
import luigi
import codecs
from luigi.contrib.postgres import CopyToTable

from data_importer.lib.config import Config
from data_importer.tasks.file.remote import RemoteFileTask
from data_importer.tasks.file.local import LocalFileTask
from data_importer.lib.db import db_create_index


class GBIFTask(CopyToTable):
    # Luigi Postgres database connections
    host = Config.get('database', 'host')
    database = Config.get('database', 'datastore_dbname')
    user = Config.get('database', 'username')
    password = Config.get('database', 'password')
    table = 'gbif'

    columns = [
        ("ID", "INT"),
        ("occurrenceID", "UUID"),
        ("lastInterpreted", "TEXT"),  # Possible date??
        ("issue", "TEXT"),
        ("kingdom", "TEXT"),
        ("kingdomKey", "INT"),
        ("phylum", "TEXT"),
        ("phylumKey", "INT"),
        ("class", "TEXT"),
        ("classKey", "INT"),
        ("taxonOrder", "TEXT"),  # Using just order breaks import
        ("orderKey", "INT"),
        ("family", "TEXT"),
        ("familyKey", "INT"),
        ("genus", "TEXT"),
        ("genusKey", "INT"),
        ("subgenus", "TEXT"),
        ("subgenusKey", "INT"),
        ("species", "TEXT"),
        ("speciesKey", "INT"),
        ("taxonRank", "TEXT"),
        ("taxonKey", "INT"),
        ("identifiedBy", "TEXT"),
        ("scientificName", "TEXT"),
        ("recordedBy", "TEXT"),
        ("eventDate", "TEXT"),
        ("recordNumber", "TEXT"),
        ("continent", "TEXT"),
        ("country", "TEXT"),
        ("countryCode", "TEXT"),
        ("stateProvince", "TEXT"),
        ("habitat", "TEXT"),
        ("islandGroup", "TEXT"),
        ("decimalLongitude", "TEXT"),
        ("decimalLatitude", "TEXT"),
    ]

    def requires(self):
        # TODO: New GBIF API does not include many of the IDs needed to link out - tmp use local file
        # return RemoteFileTask('http://api.gbif.org/v1/occurrence/download/request/0005170-170826194755519.zip')
        file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'gbif.csv.gz')
        return LocalFileTask(file_path)

    @staticmethod
    def _get_value(col, col_type, line):
        """
        Rewrite column name taxonOrder to order (order breaksSQL import)
        @param col:
        @return:
        """
        col = 'order' if col == 'taxonOrder' else col
        value = line.get(col, None)
        # Coerce empty strings to None, empty strings break on integer fields
        if not value:
            value = None

        return value

    def rows(self):
        """
        Implementation of CopyToTable.rows()
        :return:
        """
        # Populate row using the same order as columns
        with self.input().open('r') as f:
            csvfile = csv.DictReader(codecs.iterdecode(f, 'utf-8'))
            for line in csvfile:
                row = [self._get_value(col, col_type, line) for col, col_type in self.columns]
                yield row

    def on_success(self):
        """
        On completion, create indexes
        @return:
        """
        self.ensure_indexes()

    def ensure_indexes(self):
        connection = self.output().connect()
        db_create_index(self.table, 'occurrenceID', 'btree', connection)
        connection.commit()


if __name__ == "__main__":
    luigi.run(main_task_cls=GBIFTask)
