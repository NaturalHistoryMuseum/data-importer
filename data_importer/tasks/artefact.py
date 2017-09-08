#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '14/03/2017'.



"""

import luigi

from operator import eq, is_not

from data_importer.tasks.dataset import DatasetTask
from data_importer.lib.field import Field
from data_importer.lib.filter import Filter
from data_importer.lib.config import Config


class ArtefactDatasetTask(DatasetTask):

    package_name = 'collection-artefacts'
    package_description = "Cultural and historical artefacts from The Natural History Museum"
    package_title = "Artefacts"

    resource_title = 'Artefacts'
    resource_id = Config.get('resource_ids', 'artefact')
    resource_description = 'Museum Artefacts'

    record_types = ['Artefact']

    fields = DatasetTask.fields + [
        Field('ecatalogue', 'AdmGUIDPreferredValue', 'GUID'),
        Field('ecatalogue', 'PalArtObjectName', 'artefactName'),
        Field('ecatalogue', 'PalArtType', 'artefactType'),
        Field('ecatalogue', 'PalArtDescription', 'artefactDescription'),
        Field('ecatalogue', 'IdeCurrentScientificName', 'scientificName')
    ]

if __name__ == "__main__":
    luigi.run(main_task_cls=ArtefactDatasetTask)

