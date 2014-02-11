#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

from ke2psql.tasks.ke import KEDataTask
from ke2psql.model.keemu import *


class TaxonomyTask(KEDataTask):

    model_class = TaxonomyModel
    module = 'etaxonomy'

    def process(self, data):

        # Currently accepted isn't required so None == Unknown
        try:
            if data['ClaCurrentlyAccepted'] == "Unknown":
                data['ClaCurrentlyAccepted'] = None
        except KeyError:
            pass

        super(TaxonomyTask, self).process(data)





