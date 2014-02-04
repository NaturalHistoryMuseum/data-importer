#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

from ke2psql.tasks.ke import KEDataTask
from ke2psql.model.keemu import *


class CollectionEventsTask(KEDataTask):

    model_class = CollectionEventModel
    file_name = 'ecollectionevents'





