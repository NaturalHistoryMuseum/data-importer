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
        return luigi.LocalTarget(self.file_path)
