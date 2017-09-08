#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '14/03/2017'.



"""

import luigi
from ke2sql.tasks.dataset import DatasetTask
from ke2sql.lib.field import Field, ForeignKeyField
from ke2sql.lib.config import Config


class IndexLotDatasetTask(DatasetTask):

    package_name = 'collection-indexlots'
    package_description = "Index Lot records from the Natural History Museum's collection"
    package_title = "Index Lot collection"

    resource_title = 'Index Lots'
    resource_id = Config.get('resource_ids', 'indexlot')
    resource_description = 'Species level record denoting the presence of a taxon in the Museum collection'

    record_types = ['Index Lot']

    fields = DatasetTask.fields + [
        Field('ecatalogue', 'AdmGUIDPreferredValue', 'GUID'),
        Field('ecatalogue', 'EntIndMaterial', 'material'),
        Field('ecatalogue', 'EntIndType', 'type'),
        Field('ecatalogue', 'EntIndMedia', 'media'),
        Field('ecatalogue', 'EntIndBritish', 'british'),
        Field('ecatalogue', 'EntIndKindOfMaterial', 'kindOfMaterial'),
        Field('ecatalogue', 'EntIndKindOfMedia', 'kindOfMedia'),
        # Material detail
        Field('ecatalogue', 'EntIndCount', 'materialCount'),
        Field('ecatalogue', 'EntIndSex', 'materialSex'),
        Field('ecatalogue', 'EntIndStage', 'materialStage'),
        Field('ecatalogue', 'EntIndTypes', 'materialTypes'),
        Field('ecatalogue', 'EntIndPrimaryTypeNo', 'materialPrimaryTypeNumber'),
        # Etaxonomy
        Field('etaxonomy', 'ClaScientificNameBuilt', 'scientificName'),
        Field('etaxonomy', 'ClaCurrentSciNameLocal', 'currentScientificName'),
        Field('etaxonomy', 'ClaKingdom', 'kingdom'),
        Field('etaxonomy', 'ClaPhylum', 'phylum'),
        Field('etaxonomy', 'ClaClass', 'class'),
        Field('etaxonomy', 'ClaOrder', 'order'),
        Field('etaxonomy', 'ClaSuborder', 'suborder'),
        Field('etaxonomy', 'ClaSuperfamily', 'superfamily'),
        Field('etaxonomy', 'ClaFamily', 'family'),
        Field('etaxonomy', 'ClaSubfamily', 'subfamily'),
        Field('etaxonomy', 'ClaGenus', 'genus'),
        Field('etaxonomy', 'ClaSubgenus', 'subgenus'),
        Field('etaxonomy', 'ClaSpecies', 'specificEpithet'),
        Field('etaxonomy', 'ClaSubspecies', 'infraspecificEpithet'),
        Field('etaxonomy', 'ClaRank', 'taxonRank'),  # NB: CKAN uses rank internally
    ]

    foreign_keys = DatasetTask.foreign_keys + [
        ForeignKeyField('ecatalogue', 'etaxonomy', 'EntIndIndexLotTaxonNameLocalRef')
    ]

if __name__ == "__main__":
    luigi.run(main_task_cls=IndexLotDatasetTask)
