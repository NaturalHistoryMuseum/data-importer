#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '14/02/2017'.
"""
from sqlalchemy.ext.declarative import declarative_base

from ke2sql.models.mixin import MixinModel


Base = declarative_base()


class ECatalogueModel(Base, MixinModel):
    """
    Ecatalogue records
    """
    property_mappings = (
        ('AdmGUIDPreferredValue', 'occurrenceID'),
        ('DarCatalogNumber', 'catalogNumber'),
        ('DarScientificName', 'scientificName'),
        ('DarKingdom', 'kingdom'),
        ('DarPhylum', 'phylum'),
        ('DarClass', 'class'),
        ('DarOrder', 'order'),
        ('DarFamily', 'family'),
        ('DarGenus', 'genus'),
        ('DarSubgenus', 'subgenus'),
        ('DarSpecies', 'specificEpithet'),
        ('DarSubspecies', 'infraspecificEpithet'),
        ('DarHigherTaxon', 'higherClassification'),
        ('DarInfraspecificRank', 'taxonRank'),
    )