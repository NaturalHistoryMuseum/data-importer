#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '30/08/2017'.
"""

import luigi
from data_importer.lib.column import Column
from data_importer.lib.filter import Filter
from data_importer.tasks.keemu.base import KeemuBaseTask
from operator import is_not


class EMultimediaTask(KeemuBaseTask):
    """
    Task for importing the KE EMu ecatalogue module
    """
    module_name = 'emultimedia'

    # Additional columns for emultimedia module - add embargo date
    columns = KeemuBaseTask.columns + [
        Column('embargo_date', "DATE", ["NhmSecEmbargoDate", "NhmSecEmbargoExtensionDate"], True),
        # note the use of the formatter parameter to turn the value into a bool
        Column('pending', 'BOOLEAN', 'GenDigitalMediaId', True, lambda g: g == 'Pending')
    ]

    # Apply filters to each record, and do not import if any fail
    record_filters = KeemuBaseTask.record_filters + [
        Filter('GenDigitalMediaId', [
            (is_not, None)
        ]),
    ]

    # Ensure emultimedia tasks runs before FileTask as both are
    # requirements of EcatalogueTask
    priority = 100


if __name__ == "__main__":
    luigi.run(main_task_cls=EMultimediaTask)
