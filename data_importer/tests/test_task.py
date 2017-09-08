#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '03/04/2017'.
"""

import luigi

from data_importer.lib.helpers import get_dataset_tasks
from data_importer.tasks.delete import DeleteTask


class TestTask(luigi.Task):
    """
    Helper task for scheduling delete and dataset tasks
    """
    date = luigi.IntParameter()

    def requires(self):
        params = {
            'date': int(self.date)
        }
        yield DeleteTask(**params)
        params['view_only'] = True
        for task in get_dataset_tasks():
            yield task(**params)
