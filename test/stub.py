#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import sys
import os
from ke2sql.tasks import StratigraphyTask
from base import BaseTask, BaseTest
from ke2sql.model.keemu import *
import unittest
from sqlalchemy.types import Integer, String, Date

# TODO:

def main():
    model = SpecimenModel()
    aliases = model.get_aliases()

    x = 0
    done = []

    for alias, field in aliases.items():

        if field in done:
            continue

        type = getattr(SpecimenModel, field).property.columns[0].type

        if isinstance(type, String):
            val = 'V%s' % x
        elif isinstance(type, Integer):
            val = 1
        else:
            val = '1999-01-31'

        x += 1

        print '%s:1=%s' % (alias, val)

        done.append(field)


if __name__ == '__main__':
    main()