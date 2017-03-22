#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '14/03/2017'.



"""

import luigi

from operator import eq, is_not

from ke2sql.tasks.dataset import DatasetTask


class ArtefactDatasetTask(DatasetTask):

    package_name = 'collection-artefacts4'
    package_description = "Cultural and historical artefacts from The Natural History Museum"
    package_title = "Artefacts"

    resource_title = 'Artefacts'
    resource_description = 'Museum Artefacts'

    fields = DatasetTask.fields + [
        ('ecatalogue.AdmGUIDPreferredValue', 'GUID'),
        ('ecatalogue.PalArtObjectName', 'artefactName'),
        ('ecatalogue.PalArtType', 'artefactType'),
        ('ecatalogue.PalArtDescription', 'artefactDescription'),
        ('ecatalogue.IdeCurrentScientificName', 'scientificName')
    ]

    filters = {**DatasetTask.filters, **{
        # Records must have a GUID
        'ecatalogue.AdmGUIDPreferredValue': [
            (is_not, None)
        ],
        # Does this record have an excluded status - Stub etc.,
        'ecatalogue.SecRecordStatus': [
            (eq, 'Active'),
        ],
        # Col record type must be artefact
        'ecatalogue.ColRecordType': [
            (eq, 'Artefact'),
        ],
    }}

if __name__ == "__main__":
    luigi.run(main_task_cls=ArtefactDatasetTask)

