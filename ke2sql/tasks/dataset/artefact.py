#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '14/03/2017'.



"""

import luigi

from ke2sql.tasks.dataset.base import BaseDatasetTask


class ArtefactDatasetTask(BaseDatasetTask):

    record_type = 'Artefact'

    package_name = 'collection-artefacts'
    package_description = "Cultural and historical artefacts from The Natural History Museum"
    package_title = "Artefacts"

    resource_title = 'Artefacts'
    resource_description = 'Museum Artefacts'

    properties = [
        ('ecatalogue.AdmGUIDPreferredValue', 'GUID'),
        ('ecatalogue.PalArtObjectName', 'artefactName'),
        ('ecatalogue.PalArtType', 'artefactType'),
        ('ecatalogue.PalArtDescription', 'artefactDescription'),
        ('ecatalogue.IdeCurrentScientificName', 'scientificName'),
    ]

if __name__ == "__main__":
    luigi.run(main_task_cls=ArtefactDatasetTask)


#
