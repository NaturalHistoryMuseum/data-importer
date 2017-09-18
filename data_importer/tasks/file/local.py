#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import luigi
from luigi.format import Gzip


class LocalFileTask(luigi.ExternalTask):
    """
    Wrapper around a local file
    """
    file_path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.file_path, Gzip)
