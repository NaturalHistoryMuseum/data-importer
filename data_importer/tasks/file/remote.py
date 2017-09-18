#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '08/09/2017'.
"""

import os
import luigi
import requests
import tempfile
import shutil

from luigi.format import Gzip

class RemoteFileTask(luigi.ExternalTask):
    """
    Remote File Task - downloads to local tmp and returns LocalTarget
    """

    url = luigi.Parameter()
    dest = os.path.join(tempfile.gettempdir(), 'gbif.zip')

    def run(self):
        # Download file
        r = requests.get(self.url, stream=True)
        r.raise_for_status()
        with open(self.dest, 'wb') as f:
            shutil.copyfileobj(r.raw, f)

    def output(self):
        """
        Return a local file object
        @return:
        """
        return luigi.LocalTarget(self.dest, Gzip)
