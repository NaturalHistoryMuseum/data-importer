#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '23/05/2016'.
"""

import os
import requests


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
