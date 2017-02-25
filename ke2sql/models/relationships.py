#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '17/02/2017'.
"""

# from sqlalchemy import Column, String, Integer, ForeignKey, UniqueConstraint
# from sqlalchemy.ext.declarative import declared_attr

# from ke2sql.models.base import Base
# from ke2sql.models.ecatalogue import ECatalogueModel
# from ke2sql.models.emultimedia import EMultimediaModel


# __all__ = ['CatalogueEMultimediaRelationship', 'CatalogueParentRelationship']


# class CatalogueMultimediaRelationship(Base):
#     """
#     Attach multimedia records to catalogue records
#     """
#     __tablename__ = 'catalogue_multimedia_rel'
#     # Do not allow duplicate rows in the table
#     __table_args__ = (UniqueConstraint('catalogue_irn', 'multimedia_irn'),)
#     catalogue_irn = Column(Integer, ForeignKey(ECatalogueModel.irn, ondelete='CASCADE'), primary_key=True, nullable=False)
#     multimedia_irn = Column(Integer, ForeignKey(EMultimediaModel.irn), nullable=False)


# class CatalogueParentRelationship(Base):
#     """
#     Attach multimedia records to catalogue records
#     """
#     __tablename__ = 'parent_rel'
#     __table_args__ = (UniqueConstraint('child_irn', 'parent_irn'),)
#     child_irn = Column(Integer, ForeignKey(ECatalogueModel.irn, ondelete='CASCADE'), primary_key=True, nullable=False)
#     parent_irn = Column(Integer, ForeignKey(ECatalogueModel.irn, ondelete='CASCADE'), nullable=False)


# class CatalogueParasiteRelationship(Base):
#     """
#     Attach multimedia records to catalogue records
#     """
#     __tablename__ = 'parasite_rel'
#     __table_args__ = (UniqueConstraint('child_irn', 'parent_irn'),)
#     parasite_irn = Column(Integer, ForeignKey(ECatalogueModel.irn, ondelete='CASCADE'), primary_key=True, nullable=False)
#     taxonomy_irn = Column(Integer, ForeignKey(ECatalogueModel.irn, ondelete='CASCADE'), nullable=False)
