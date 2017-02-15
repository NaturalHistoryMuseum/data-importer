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
from sqlalchemy.ext.declarative import declarative_base
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
    # deleted = Column(Integer, nullable=True)
    properties = Column(JSONB, nullable=True)

    @abstractproperty
    def property_mappings(self):
        return None

    def is_importable(self, record):
        """
        Evaluate whether a record is importable
        At the very least a record will need AdmPublishWebNoPasswordFlag set to Y,
        Additional models will extend this to provide additional filters
        :param record:
        :return: boolean - false if not importable
        """
        if record.AdmPublishWebNoPasswordFlag.lower() != 'y':
            return False

        today_timestamp = time.time()
        embargo_dates = [
            getattr(record, 'NhmSecEmbargoDate', None),
            getattr(record, 'NhmSecEmbargoExtensionDate', None)
        ]
        for embargo_date in embargo_dates:
            if embargo_date:
                embargo_date_timestamp = time.mktime(datetime.datetime.strptime(embargo_date, "%Y-%m-%d").timetuple())
                if embargo_date_timestamp > today_timestamp:
                    return False

        return True

    def get_properties(self, record):
        """
        Method to build a dict of properties
        :param record:
        :return:
        """
        return {alias: getattr(record, field, None) for (field, alias) in self.property_mappings if getattr(record, field, None)}
