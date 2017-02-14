#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '14/02/2017'.
"""

import os
import re
import gzip
import configparser

from ke2sql.record import Record


def config():
    """
    Get config file
    :return:
    """
    cfg = configparser.ConfigParser()
    cfg.read(os.path.join(os.path.dirname(__file__), 'config.cfg'))
    return cfg


def get_records(file_path):
    """
    Iterator for extracting records from a file
    :param file_path:
    :return:
    """
    record = Record()
    match = re.compile(':([0-9]?)+')
    for line in gzip.open(file_path, 'rt', newline='\n'):

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
            field_name = match.sub('', field_name)
            setattr(record, field_name, value)
