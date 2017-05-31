#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '31/05/2017'.
"""

import click
import logging
import time
import json
from ke2sql.lib.solr import SolrIndex
from ke2sql.lib.config import Config


logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
logger = logging.getLogger('luigi-interface')


# Time to wait before checking the import has worked - by default 1 minute
SLEEP_INTERVAL = 2


@click.command()
@click.option('--full-import',  default=False, help='Perform a full index - if not set will perform delta import.', is_flag=True)
def data_import(full_import):

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


if __name__ == "__main__":
    data_import()
