#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '14/03/2017'.



"""

import luigi

from ke2sql.tasks.dataset.base import BaseDatasetTask


class IndexLotDatasetTask(BaseDatasetTask):

    record_types = ['Index Lot']

    package_name = 'collection-indexlots'
    package_description = "Index Lot records from the Natural History Museum's collection"
    package_title = "Index Lot collection"

    resource_title = 'Index Lots'
    resource_description = 'Species level record denoting the presence of a taxon in the Museum collection'

    fields = [
        ('ecatalogue.AdmGUIDPreferredValue', 'GUID'),
        ('ecatalogue.EntIndMaterial', 'material'),
        ('ecatalogue.EntIndType', 'type'),
        ('ecatalogue.EntIndMedia', 'media'),
        ('ecatalogue.EntIndBritish', 'british'),
        ('ecatalogue.EntIndKindOfMaterial', 'kindOfMaterial'),
        ('ecatalogue.EntIndKindOfMedia', 'kindOfMedia'),
        # Material detail
        ('ecatalogue.EntIndCount', 'materialCount'),
        ('ecatalogue.EntIndSex', 'materialSex'),
        ('ecatalogue.EntIndStage', 'materialStage'),
        ('ecatalogue.EntIndTypes', 'materialTypes'),
        ('ecatalogue.EntIndPrimaryTypeNo', 'materialPrimaryTypeNumber'),
        # Etaxonomy
        ('etaxonomy.ClaScientificNameBuilt', 'scientificName'),
        ('etaxonomy.ClaCurrentSciNameLocal', 'currentScientificName'),
        ('etaxonomy.ClaKingdom', 'kingdom'),
        ('etaxonomy.ClaPhylum', 'phylum'),
        ('etaxonomy.ClaClass', 'class'),
        ('etaxonomy.ClaOrder', 'order'),
        ('etaxonomy.ClaSuborder', 'Suborder'),
        ('etaxonomy.ClaSuperfamily', 'Superfamily'),
        ('etaxonomy.ClaFamily', 'Family'),
        ('etaxonomy.ClaSubfamily', 'Subfamily'),
        ('etaxonomy.ClaGenus', 'genus'),
        ('etaxonomy.ClaSubgenus', 'subgenus'),
        ('etaxonomy.ClaSpecies', 'specificEpithet'),
        ('etaxonomy.ClaSubspecies', 'infraspecificEpithet'),
        ('etaxonomy.ClaRank', 'taxonRank'),  # NB: CKAN uses rank internally
        # Join field
        ('ecatalogue.EntIndIndexLotTaxonNameLocalRef', 'indexlot_taxonomy_irn'),
    ]

if __name__ == "__main__":
    luigi.run(main_task_cls=IndexLotDatasetTask)
