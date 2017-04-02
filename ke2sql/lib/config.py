#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '20/02/2017'.
"""

import os
import six

try:
    import configparser
except ImportError:
    from six.moves import configparser

Config = configparser.ConfigParser()
config_files = ['default.cfg']

# Add test config if test flag is set
if os.environ.get('TEST', False):
    config_files.append('test.cfg')

# Build config, looping through all the file options
config_dir = os.path.dirname(os.path.dirname(__file__))
for config_file in config_files:
    Config.read(os.path.join(config_dir, config_file))
