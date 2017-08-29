#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '22/03/2017'.
"""


class Field(object):
    """
    Field definition
    :param module_name: keemu module
    :param field_name: keemu field name
    :param field_alias: the field name in the dataset
    """
    def __init__(self, module_name, field_name, field_alias):
        self.module_name = module_name
        self.field_name = field_name
        self.field_alias = field_alias


class MetadataField(Field):
    """
    Field definition
    :param module_name: keemu module
    :param field_name: keemu field name
    :param field_alias: the field name in the dataset
    :param field_type: type of field
    """
    def __init__(self, module_name, field_name, field_alias, field_type):
        super(MetadataField, self).__init__(module_name, field_name, field_alias)
        self.field_type = field_type


class ForeignKeyField(Field):
    """
    Field definition
    :param module_name: keemu module
    :param field_name: keemu field name
    :param field_alias: the field name in the dataset
    """
    pass
