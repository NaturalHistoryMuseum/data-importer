#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '30/08/2017'.
"""

import luigi
from operator import is_not

from ke2sql.tasks.keemu.base import KeemuBaseTask
from ke2sql.tasks.keemu.etaxonomy import ETaxonomyTask
from ke2sql.tasks.keemu.emultimedia import EMultimediaTask
from ke2sql.tasks.keemu.file import FileTask
from ke2sql.lib.column import Column
from ke2sql.lib.operators import is_one_of, is_not_one_of
from ke2sql.lib.filter import Filter
from ke2sql.lib.dataset import dataset_get_tasks


class EcatalogueTask(KeemuBaseTask):
    """
    Task for importing the KE EMu ecatalogue module
    """
    module_name = 'ecatalogue'

    # Additional columns for this module
    columns = KeemuBaseTask.columns + [
        Column("record_type", "TEXT", "ColRecordType", True),
        Column('embargo_date', "DATE", ["NhmSecEmbargoDate", "NhmSecEmbargoExtensionDate"]),
    ]

    @property
    def record_filters(self):
        """
        Table name is just module name, but is required by LuigiCopyToTable
        :return: string
        """
        return KeemuBaseTask.record_filters + [
            Filter('AdmGUIDPreferredValue', [
                (is_not, None)
            ]),
            # Does this record have an excluded status - Stub etc.,
            Filter('SecRecordStatus', [
                (is_not_one_of, [
                    "DELETE",
                    "DELETE-MERGED",
                    "DUPLICATION",
                    "Disposed of",
                    "FROZEN ARK",
                    "INVALID",
                    "POSSIBLE TYPE",
                    "PROBLEM",
                    "Re-registered in error",
                    "Reserved",
                    "Retired",
                    "Retired (see Notes)",
                    "Retired (see Notes)Retired (see Notes)",
                    "SCAN_cat",
                    "See Notes",
                    "Specimen missing - see notes",
                    "Stub",
                    "Stub Record",
                    "Stub record"
                ])
            ]),
            # Make sure ecatalogue records are one of the record types
            # used in one of the datasets - otherwise pulling in a load of cruft
            Filter('ColRecordType', [
                (is_one_of, self.get_record_types())
            ]),
            # Record must be in one of the known collection departments
            # (Otherwise the home page breaks) - both Artefacts & Index Lots
            # Have ColDepartment so this filter does not need to be more specific
            Filter('ColDepartment', [
                (is_one_of, [
                    "Botany",
                    "Entomology",
                    "Mineralogy",
                    "Palaeontology",
                    "Zoology"
                ])
            ]),
        ]

    @staticmethod
    def get_record_types():
        """
        Loop through all of the datasets, and get all record types
        @return: list of record types
        """
        record_types = []
        for dataset_task in dataset_get_tasks():
            record_types += dataset_task.record_types
        return record_types

    def requires(self):
        return [
            super(EcatalogueTask, self).requires(),
            ETaxonomyTask(date=self.date, limit=self.limit),
            EMultimediaTask(date=self.date, limit=self.limit)
        ]

    @property
    def file_input(self):
        """
        Ecatalogue has multiple requirements - loop through and return
        The file task input
        @return: input file ref
        """
        for i in self.input():
            if hasattr(i, 'path'):
                return i

if __name__ == "__main__":
    luigi.run(main_task_cls=EcatalogueTask)
