#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '30/08/2017'.
"""

import luigi
from operator import is_not

from data_importer.tasks.keemu.base import KeemuBaseTask
from data_importer.lib.column import Column
from data_importer.lib.operators import is_one_of, is_not_one_of
from data_importer.lib.filter import Filter
from data_importer.lib.dataset import dataset_get_tasks


class ETaxonomyTask(KeemuBaseTask):
    """
    Task for importing the KE EMu etaxonomy module
    """
    module_name = 'etaxonomy'

    # Ensure etaxonomy tasks runs before FileTask as both are
    # requirements of EcatalogueTask
    priority = 100

if __name__ == "__main__":
    luigi.run(main_task_cls=ETaxonomyTask)
