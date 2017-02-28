#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '14/02/2017'.
"""

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, Column, Integer, Float, String, ForeignKey, Boolean, Date, UniqueConstraint, Enum, DateTime, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy import inspect

class Base(object):
    """
    Model Base for defining core fields
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
        extra_fields = []
        # Reference to the Declarative Base object - this object, where self is the child class
        this = self.__class__.__bases__[0]
        # Loop through all the columns defined in the child model - if it doesn't
        # exist in this base class, it's an extra field
        for prop in self.__mapper__.columns:
            if not hasattr(this, prop.key):
                extra_fields.append(prop.key)
        return extra_fields


Base = declarative_base(cls=Base)
