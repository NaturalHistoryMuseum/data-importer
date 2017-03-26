#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '22/03/2017'.
"""


class Filter(object):
    """
    Field definition
    :param module_name: keemu module
    :param field_name: keemu field name
    :param filters: List of filters to apply
    """
    def __init__(self, module_name, field_name, filters):
        self.module_name = module_name
        self.field_name = field_name
        self.filters = filters
