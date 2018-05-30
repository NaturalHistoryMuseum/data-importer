#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '22/03/2017'.
"""


class Column(object):
    """
    Field definition
    :param module_name: keemu module
    :param field_name: field name
    param field_name: postgres field type
    :param field_alias: the keemu field
    :param formatter: a function that when given a value returns a formatted
    version appropriate for this column. If no formatter is provided the value
    is simply returned as is.
    """
    def __init__(self, field_name, field_type, ke_field_name=None,
                 indexed=False, formatter=lambda v: v):
        self.field_name = field_name
        self.field_type = field_type
        self.ke_field_name = ke_field_name
        self.indexed = indexed
        self.formatter = formatter

    def get_index_type(self):
        """
        Get index type - we use GIN for JSONB fields; BTREE for all others
        @return:
        """

        return 'GIN' if self.field_type == 'JSONB' else 'BTREE'
