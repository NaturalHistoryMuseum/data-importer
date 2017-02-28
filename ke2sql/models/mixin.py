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

    def get_extra_fields(self):
        """
        Return list of extra fields defined in a child model
        :return:
        """
        print(self.__table__.columns)
        # print(self.__mapper__.columns)

    @property
    def sql(self):
        """
        Special SQL for insert / updates
        To make this faster, we don't use the ORM for inserts
        :return: SQL
        """
        return """
            INSERT INTO {0} (irn, properties, created) VALUES (%(irn)s, %(properties)s, NOW())
            ON CONFLICT (irn)
            DO UPDATE SET (properties, modified) = (%(properties)s, NOW()) WHERE {0}.irn = %(irn)s
    """.format(self.__tablename__)

    @abstractproperty
    def property_mappings(self):
        return None

