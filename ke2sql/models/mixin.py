#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '14/02/2017'.
"""
from abc import abstractproperty
import datetime
import time
from sqlalchemy import Table, Column, Integer, Float, String, ForeignKey, Boolean, Date, UniqueConstraint, Enum, DateTime, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declared_attr


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

    # @property
    # def sql(self):
    #     """
    #     Special SQL for insert / updates
    #     For speed, do not use ORM
    #     :return:
    #     """
    #     return """
    #         INSERT INTO {0} (irn, properties, created) VALUES (%(irn)s, %(properties)s, NOW())
    #         ON CONFLICT (irn)
    #         DO UPDATE SET (properties, modified) = (%(properties)s, NOW()) WHERE {0}.irn = %(irn)s
    # """.format(self.__tablename__)

    @abstractproperty
    def property_mappings(self):
        return None

    def get_properties(self, record):
        """
        Method to build a dict of properties
        :param record:
        :return:
        """
        return {alias: getattr(record, field, None) for (field, alias) in self.property_mappings if getattr(record, field, None)}
