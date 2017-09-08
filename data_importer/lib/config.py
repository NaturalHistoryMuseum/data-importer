#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '20/02/2017'.
"""

import os
import sys

try:
    import configparser
except ImportError:
    from six.moves import configparser

Config = configparser.ConfigParser()
config_files = ['default.cfg']

# Add test config if test flag is set
if 'unittest' in sys.argv[0]:
    config_files.append('test.cfg')

# Build config, looping through all the file options
config_dir = os.path.dirname(os.path.dirname(__file__))
for config_file in config_files:
    Config.read(os.path.join(config_dir, config_file))
