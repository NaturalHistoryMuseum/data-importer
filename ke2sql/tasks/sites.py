#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

from ke2psql.tasks.ke import KEDataTask
from ke2psql.model.keemu import *


class SitesTask(KEDataTask):

    model_class = SiteModel
    module = 'esites'

    def process(self, data):

        # Initially, we're only going to use single point locations
        # If we have a centroid, use that one
        # If we don't, and we do have multiple points, just use the first one
        for field, centroid_field in [
            ('LatLatitude', 'LatCentroidLatitude'),
            ('LatLatitudeDecimal', 'LatCentroidLatitudeDec'),
            ('LatLongitude', 'LatCentroidLongitude'),
            ('LatLongitudeDecimal', 'LatCentroidLongitudeDec')
        ]:

            centroid_value = data.get(centroid_field, None)

            # If we have a centroid value, use that
            if centroid_value:
                data[field] = centroid_value
            else:
                # Otherwise if it's a list, use the first value
                value = data.get(field, None)

                if isinstance(value, list):
                    data[field] = value[0]

        super(SitesTask, self).process(data)





