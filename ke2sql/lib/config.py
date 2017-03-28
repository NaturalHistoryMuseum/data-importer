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
Config.read(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.cfg'))
