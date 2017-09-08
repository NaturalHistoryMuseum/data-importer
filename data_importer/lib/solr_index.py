#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '04/09/2017'.
"""

import os
import requests


class SolrIndex:
    """
    Class for handling solr indexes
        Public api:
            full_import - run full-import command
            delta_import - run delta-import command
            status - get status of full-import command
    """

    def __init__(self, host, core_name):
        self.host = host
        self.core_name = core_name

    def _request(self, command):
        # We want the JSON response
        params = {
            'wt': 'json',
            'json.nl': 'map',
            'command': command
        }
        url = os.path.join(self.host, 'solr', self.core_name, 'dataimport')
        r = requests.get(url, params=params)
        r.raise_for_status()
        return r.json()

    def full_import(self):
        return self._request('full-import')

    def delta_import(self):
        return self._request('delta-import')

    def status(self):
        return self._request('status')
