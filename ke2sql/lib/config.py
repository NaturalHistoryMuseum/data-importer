#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '20/02/2017'.
"""

import os
import configparser


def get_config():
    """
    Get config file
    :return:
    """
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.cfg'))
    return config
