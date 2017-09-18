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

    def __init__(self, module_name, field_name, field_alias, formatter=None):
        self.module_name = module_name
        self.field_name = field_name if isinstance(field_name, list) else [field_name]
        self.field_alias = field_alias
        self.formatter = formatter

    def _get_value(self, record):
        """
        Loop through all the field names, returning value if it exists
        @param record:
        @return:
        """
        for fn in self.field_name:
            if getattr(record, fn, None):
                return getattr(record, fn)
        return None

    def get_value(self, record):
        v = self._get_value(record)
        if self.formatter:
            v = self.formatter(v)
        return v

    def has_value(self, record):
        return self._get_value(record)