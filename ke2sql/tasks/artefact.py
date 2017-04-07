#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '14/03/2017'.



"""

import luigi

from operator import eq, is_not

from ke2sql.tasks.dataset import DatasetTask
from ke2sql.lib.field import Field
from ke2sql.lib.filter import Filter
from ke2sql.lib.config import Config


class ArtefactDatasetTask(DatasetTask):

    package_name = 'collection-artefacts7'
    package_description = "Cultural and historical artefacts from The Natural History Museum"
    package_title = "Artefacts"

    resource_title = 'Artefacts'
    resource_id = Config.get('resource_ids', 'artefact')
    resource_description = 'Museum Artefacts'

    fields = DatasetTask.fields + [
        Field('ecatalogue', 'AdmGUIDPreferredValue', 'GUID'),
        Field('ecatalogue', 'PalArtObjectName', 'artefactName'),
        Field('ecatalogue', 'PalArtType', 'artefactType'),
        Field('ecatalogue', 'PalArtDescription', 'artefactDescription'),
        Field('ecatalogue', 'IdeCurrentScientificName', 'scientificName')
    ]

    filters = DatasetTask.filters + [
        # Records must have a GUID
        Filter('ecatalogue', 'AdmGUIDPreferredValue', [
            (is_not, None)
        ]),
        # Does this record have an excluded status - Stub etc.,
        Filter('ecatalogue', 'SecRecordStatus', [
            (eq, 'Active'),
        ]),
        # Col record type must be artefact
        Filter('ecatalogue', 'ColRecordType', [
            (eq, 'Artefact'),
        ]),
    ]

    sql = """
        SELECT cat.irn as _id,
        cat.properties,
        (SELECT jsonb_agg(properties)
          FROM emultimedia
          WHERE emultimedia.deleted IS NULL AND (emultimedia.embargo_date IS NULL OR emultimedia.embargo_date < NOW()) AND emultimedia.irn = ANY(cat.multimedia_irns))
        AS multimedia
        FROM ecatalogue cat
        WHERE
          cat.record_type = 'Artefact'
          AND (cat.embargo_date IS NULL OR cat.embargo_date < NOW())
          AND cat.deleted IS NULL
    """

if __name__ == "__main__":
    luigi.run(main_task_cls=ArtefactDatasetTask)

