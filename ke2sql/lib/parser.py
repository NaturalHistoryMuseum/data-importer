#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '25/02/2017'.
"""

import re
import gzip
from ke2sql.lib.record import Record


class Parser(object):
    """
    Iterator for parsing a KE EMu export file
    Yields record objects
    """
    def __init__(self, path):
        self.path = path
        self.export_file = gzip.open(self.path, 'rt')
        self.re_field_name_index = re.compile(':([0-9]?)+')

    def __iter__(self):
        return self

    def __next__(self):
        for record in self._parse():
            return record
        raise StopIteration()

    def _parse(self):
        record = Record()
        for line in self.export_file:
            line = line.strip()
            if not line:
                continue
            # If is a line separator, write the record
            if line == '###':
                yield record
                record = Record()
            else:
                field_name, value = line.split('=', 1)
                # Replace field name indexes
                field_name = self.re_field_name_index.sub('', field_name)
                setattr(record, field_name, value)