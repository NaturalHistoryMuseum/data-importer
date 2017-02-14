#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '14/02/2017'.
"""

from abc import abstractproperty, abstractmethod
from sqlalchemy import Table, Column, Integer, Float, String, ForeignKey, Boolean, Date, UniqueConstraint, Enum, DateTime, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.declarative import declared_attr

Base = declarative_base()


class MixinModel(object):
    """
    Base Mixin for defining core fields
    """
    @declared_attr
    def __tablename__(cls):
        """
        Table name - lower case class with model removed
        :return:
        """
        return cls.__name__.lower().replace('model', '')

    irn = Column(Integer, primary_key=True)
    created = Column(DateTime, nullable=False, server_default=func.now())
    modified = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    deleted = Column(DateTime, nullable=True)
    properties = Column(JSONB, nullable=True)

    @abstractproperty
    def property_mappings(self):
        return None

    @staticmethod
    def is_importable(record):
        """
        Evaluate whether a record is importable
        At the very least a record will need AdmPublishWebNoPasswordFlag set to Y,
        Additional models will extend this to provide additional filters
        :param record:
        :return: boolean - false if not importable
        """
        if record.AdmPublishWebNoPasswordFlag.lower() != 'y':
            return False

        # for f in ['NhmSecEmbargoDate', 'NhmSecEmbargoExtensionDate']:

        return True

    def get_properties(self, record):
        """
        Method to build a dict of properties
        :param record:
        :return:
        """
        return {alias: getattr(record, field, None) for (field, alias) in self.property_mappings if getattr(record, field, None)}


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

# class ESitesModel(Base, MixinModel):
#     """
#     Esites records
#     """

