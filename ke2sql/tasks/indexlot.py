#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '14/03/2017'.



"""

import luigi
from operator import eq, is_not
from ke2sql.tasks.dataset import DatasetTask
from ke2sql.lib.field import Field, MetadataField
from ke2sql.lib.filter import Filter
from ke2sql.lib.config import Config


class IndexLotDatasetTask(DatasetTask):

    package_name = 'collection-indexlots'
    package_description = "Index Lot records from the Natural History Museum's collection"
    package_title = "Index Lot collection"

    resource_title = 'Index Lots'
    resource_id = Config.get('resource_ids', 'indexlot')
    resource_description = 'Species level record denoting the presence of a taxon in the Museum collection'

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
        Field('etaxonomy', 'ClaSuborder', 'Suborder'),
        Field('etaxonomy', 'ClaSuperfamily', 'Superfamily'),
        Field('etaxonomy', 'ClaFamily', 'Family'),
        Field('etaxonomy', 'ClaSubfamily', 'Subfamily'),
        Field('etaxonomy', 'ClaGenus', 'genus'),
        Field('etaxonomy', 'ClaSubgenus', 'subgenus'),
        Field('etaxonomy', 'ClaSpecies', 'specificEpithet'),
        Field('etaxonomy', 'ClaSubspecies', 'infraspecificEpithet'),
        Field('etaxonomy', 'ClaRank', 'taxonRank'),  # NB: CKAN uses rank internally
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
        Filter('ecatalogue', 'ColRecordType', [
            (eq, 'Index Lot'),
        ])
    ]

    # Index Lot records do not have their taxonomy populated, provide mechanism to join
    metadata_fields = DatasetTask.metadata_fields + [
        MetadataField('ecatalogue', 'EntIndIndexLotTaxonNameLocalRef', 'indexlot_taxonomy_irn', "INTEGER"),
    ]

    def get_query(self):
        """
        Override get_query to add join to etaxonomy table
        :return:
        """
        query = super(IndexLotDatasetTask, self).get_query()
        # Add the join to the etaxonomy module
        query.insert_after(
            ['FROM', 'ecatalogue'],
            ['LEFT JOIN', 'etaxonomy', 'ON', 'etaxonomy.irn = ' + self.table + '.indexlot_taxonomy_irn', 'AND', 'etaxonomy.deleted IS NULL']
        )
        return query


if __name__ == "__main__":
    luigi.run(main_task_cls=IndexLotDatasetTask)
