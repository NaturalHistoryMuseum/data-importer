#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '30/08/2017'.
"""

import abc
import luigi
from operator import is_not
from operator import is_not, ne

from ke2sql.tasks.keemu.base import KeemuBaseTask
from ke2sql.lib.column import Column
from ke2sql.lib.operators import is_one_of, is_not_one_of
from ke2sql.lib.filter import Filter


class EMultimediaTask(KeemuBaseTask):
    """
    Task for importing the KE EMu ecatalogue module
    """
    module_name = 'emultimedia'

    # Additional columns for emultimedia module - add embargo date
    columns = KeemuBaseTask.columns + [
        Column('embargo_date', "DATE", ["NhmSecEmbargoDate", "NhmSecEmbargoExtensionDate"], True),
    ]

    # Apply filters to each record, and do not import if any fail
    record_filters = KeemuBaseTask.record_filters + [
        Filter('GenDigitalMediaId', [
            (is_not, None),
            (ne, 'Pending')
        ]),
    ]

    # Ensure emultimedia tasks runs before FileTask as both are
    # requirements of EcatalogueTask
    priority = 100


if __name__ == "__main__":
    luigi.run(main_task_cls=EMultimediaTask)
