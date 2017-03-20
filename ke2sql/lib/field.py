#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '25/02/2017'.
"""

import re
import gzip
from ke2sql.lib.record import Record


class Field(object):
    """
    Iterator for parsing a KE EMu export file
    Yields record objects
    """
    def __init__(self, module_name, keemu_field, dataset_field):
        self.module_name = module_name
        self.keemu_field = keemu_field
        self.dataset_field = dataset_field
