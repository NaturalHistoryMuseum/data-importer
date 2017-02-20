#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '14/02/2017'.
"""

from ke2sql.models.mixin import MixinModel
from ke2sql.models.base import Base


class TaxonomyModel(Base, MixinModel):
    """
    ETaxonomy records
    """
    property_mappings = (
        ('ClaScientificNameBuilt', 'scientificName'),
        ('ClaKingdom', 'kingdom'),
        ('ClaPhylum', 'phylum'),
        ('ClaClass', 'class'),
        ('ClaOrder', 'order'),
        ('ClaSuborder', 'Suborder'),
        ('ClaSuperfamily', 'Superfamily'),
        ('ClaFamily', 'Family'),
        ('ClaSubfamily', 'Subfamily'),
        ('ClaGenus', 'genus'),
        ('ClaSubgenus', 'subgenus'),
        ('ClaSpecies', 'specificEpithet'),
        ('ClaSubspecies', 'infraspecificEpithet'),
        ('ClaRank', 'taxonRank')  # NB: CKAN uses rank internally
    )
