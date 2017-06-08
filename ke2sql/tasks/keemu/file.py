#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import os
import luigi
from ke2sql.lib.config import Config


class FileTask(luigi.ExternalTask):
    """
    Wrapper around a KE EMu export file
    Luigi requires LocalTarget tasks to be  external tasks
    """
    date = luigi.IntParameter()
    file_name = luigi.Parameter()

    @property
    def file_path(self):
        file_name = '{file_name}.{date}.gz'.format(
            file_name=self.file_name,
            date=self.date
        )
        return os.path.join(Config.get('keemu', 'export_dir'), file_name)

    def output(self):
        # Check file size if greater than zero
        # Will also raise an error if the file doesn't exist
        file_size = os.path.getsize(self.file_path)
        if file_size == 0:
            raise IOError('KE EMu export file %s has zero byte length' % self.file_path)
        return luigi.LocalTarget(self.file_path)
