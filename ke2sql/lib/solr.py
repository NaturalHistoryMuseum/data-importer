#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '23/05/2016'.
"""

import time
import json
import os
import requests
from ke2sql.lib.config import Config


# Time to wait before checking the import has worked - by default 1 minute
SLEEP_INTERVAL = 2
CORE_NAME = 'specimen_collection'

class SolrIndex:
    """
    Class for handling solr indexes
        Public api:
            full_import - run full-import command
            status - get status of full-import command
    """

    def __init__(self, index):
        self.index = index

    def _request(self, command):
        # We want the JSON response
        params = {
            'wt': 'json',
            'json.nl': 'map',
            'command': command
        }
        url = os.path.join(self.index, 'solr', CORE_NAME, 'dataimport')
        r = requests.get(url, params=params)
        r.raise_for_status()
        return r.json()

    def full_import(self):
        return self._request('full-import')

    def delta_import(self):
        return self._request('delta-import')

    def status(self):
        return self._request('status')


def solr_reindex():
    solr_hosts = json.loads(Config.get('solr', 'hosts'))
    # Loop through the cores, request a full import and wait until it completes before
    # requesting for the next index - ensures there's always a stable index available for requests
    for solr_host in solr_hosts:
        solr_index = SolrIndex(solr_host)
        print("Starting delta import of index: %s" % solr_host)
        solr_index.delta_import()
        # Enter loop to keep checking status every SLEEP_INTERVAL
        while True:
            r = solr_index.status()
            if r['status'] == 'busy':
                print('Total Rows Fetched: %s' % r['statusMessages'].get('Total Rows Fetched'))
                print('Time elapsed: %s' % r['statusMessages'].get('Time Elapsed'))
                time.sleep(SLEEP_INTERVAL)
            else:
                print(r['statusMessages'].get(''))
                print('Time taken: %s' % r['statusMessages'].get('Time taken'))
                break


if __name__ == "__main__":
    solr_reindex()
