#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '04/09/2017'.
"""

import json
import luigi
import time
import logging
from datetime import datetime

from data_importer.lib.solr_index import SolrIndex
from data_importer.lib.config import Config
from data_importer.tasks.postgres import PostgresTask

logger = logging.getLogger('luigi-interface')


class SolrIndexTask(PostgresTask):
    # Name of the solr core - e.g. collection-specimens
    core = luigi.Parameter()

    # Interval to wait before checking import has completed
    sleep_interval = 2

    table = 'ecatalogue'

    def __init__(self, *args, **kwargs):
        super(SolrIndexTask, self).__init__(*args, **kwargs)
        solr_hosts = json.loads(Config.get('solr', 'hosts'))
        self.indexes = []
        for solr_host in solr_hosts:
            self.indexes.append(SolrIndex(solr_host, self.core))

    @staticmethod
    def _last_import_date(solr_index):
        r = solr_index.status()
        for i in ['Full Dump Started', 'Delta Dump started']:
            d = r['statusMessages'].get(i, None)
            if d:
                return datetime.strptime(d, '%Y-%m-%d %H:%M:%S')
        return None

    def complete(self):
        """
        Completion is based on whether indexing has run today, and indexed at least one document
        @return:
        """
        connection = self.output().connect()
        cursor = connection.cursor()
        query = "SELECT 1 from {table} WHERE created>%s OR modified>%s LIMIT 1".format(
            table=self.table
        )

        for solr_index in self.indexes:
            last_import = self._last_import_date(solr_index)
            if not last_import:
                return False
            # Do we have new records since last index operation ran
            cursor.execute(query, (last_import, last_import))
            if cursor.fetchone():
                return False

        return True

    def run(self):
        for solr_index in self.indexes:
            # We always call delta import - if this is the first run won't make any difference
            solr_index.delta_import()
            while True:
                r = solr_index.status()
                if r['status'] == 'busy':
                    logger.info('Total Rows Fetched: %s', r['statusMessages'].get('Total Rows Fetched'))
                    logger.info('Time Elapsed: %s', r['statusMessages'].get('Time Elapsed'))
                    time.sleep(self.sleep_interval)
                else:
                    logger.info('Total Rows: %s', r['statusMessages'].get('Total Rows Fetched'))
                    logger.info('Time taken: %s', r['statusMessages'].get('Time taken'))
                    break
