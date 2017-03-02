#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '14/02/2017'.
"""

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, Column, Integer, Float, String, ForeignKey, Boolean, Date, UniqueConstraint, Enum, DateTime, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declared_attr
from abc import abstractproperty


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
                extra_fields.append(prop)
        return extra_fields

    @property
    def sql(self):
        """
        Special SQL for insert / updates
        To make this faster, we don't use the ORM for inserts
        :return: SQL
        """

        extra_fields = [f.key for f in self.get_extra_fields()]
        insert_fields = ['irn', 'properties'] + extra_fields
        update_fields = ['properties'] + extra_fields

        return """
            INSERT INTO {table_name} ({insert_fields}, created) VALUES ({insert_fields_placeholders}, NOW())
            ON CONFLICT (irn)
            DO UPDATE SET ({update_fields}, modified) = ({update_fields_placeholders}, NOW()) WHERE {table_name}.irn = %(irn)s
    """.format(
            table_name=self.__tablename__,
            insert_fields=','.join(insert_fields),
            insert_fields_placeholders=','.join(map(lambda field: "%({0})s".format(field), insert_fields)),
            update_fields=','.join(update_fields),
            update_fields_placeholders=','.join(map(lambda field: "%({0})s".format(field), update_fields)),
        )

    @property
    def delete_sql(self):
        """
        SQL for marking a record as deleted
        :return: SQL
        """
        return "UPDATE {table_name} SET (deleted) = (NOW()) WHERE {table_name}.irn = %(irn)s".format(
            table_name=self.__tablename__,
        )

    @abstractproperty
    def property_mappings(self):
        return None

Base = declarative_base(cls=Base)
