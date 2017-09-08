#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '22/03/2017'.
"""


class Filter(object):
    """
    Field definition
    :param field_name: keemu field name
    :param filters: List of filters to apply
    """
    def __init__(self, field_name, filters):
        self.field_name = field_name
        self.filters = filters

    def apply(self, record):
        value = getattr(record, self.field_name, None)
        for filter_operator, filter_value in self.filters:
            if not filter_operator(value, filter_value):
                return False
        return True

    def __str__(self):
        return '%s - %s' % (self.field_name, self.filters)
