#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '04/09/2017'.
"""

import luigi

from ke2sql.lib.solr_index import SolrIndex
from ke2sql.lib.solr import solr_get_schema, solr_get_query


class SolrIndexTask(luigi.Task):
    def run(self):

        # Time to wait before checking the import has worked - by default 1 minute
        SLEEP_INTERVAL = 2

        solr_hosts = json.loads(Config.get('solr', 'hosts'))
        # Loop through the cores, request a full import and wait until it completes before
        # requesting for the next index - ensures there's always a stable index available for requests
        for solr_host in solr_hosts:
            solr_index = SolrIndex(solr_host)
            logger.info("Starting {import_type} import of index: {solr_host}".format(
                import_type='full' if full_import else 'delta',
                solr_host=solr_host
            ))
            if full_import:
                solr_index.full_import()
            else:
                solr_index.delta_import()
            # Enter loop to keep checking status every SLEEP_INTERVAL
            while True:
                r = solr_index.status()
                if r['status'] == 'busy':
                    logger.info('Total Rows Fetched: %s', r['statusMessages'].get('Total Rows Fetched'))
                    logger.info('Time Elapsed: %s', r['statusMessages'].get('Time Elapsed'))
                    time.sleep(SLEEP_INTERVAL)
                else:
                    logger.info('Total Rows: %s', r['statusMessages'].get('Total Rows Fetched'))
                    logger.info('Time taken: %s', r['statusMessages'].get('Time taken'))
                    break


        solr_index = SolrIndex('http://10.11.20.11:8080', 'specimen-collection')
        # print(solr_index.status())

        # Check index
        # Get existing index

        # Build current index
        # schema = self.get_solr_schema()

        print("SOLR")

    def get_solr_schema(self):
        schema = solr_get_schema(self.__class__)

    def old(self):
        CORE_NAME = 'specimen_collection'


