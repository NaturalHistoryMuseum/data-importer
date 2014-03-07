#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import sys
import os
from ke2sql.model import meta
from ke2sql.model.base import Base
from sqlalchemy import Table

# Reflect the table. It's purpose is as a view, but this is faster
SpecimenTaxonomyView = Table("specimen_taxonomy", Base.metadata, autoload=True, autoload_with=meta.engine)

