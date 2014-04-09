#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import sys
import itertools
from ke2sql.tasks.ke import KEDataTask
from ke2sql.model.keemu import *


class SitesTask(KEDataTask):

    model_class = SiteModel
    module = 'esites'

    def process(self, data):
        
        # Dictionary of fields, keyed by field name with a list of preferred fields
        # Try and use PreferredX first, followed by centroid, and then the field name
        fields = {
            'LatLatitude': ['LatPreferredCentroidLatitude', 'LatCentroidLatitude', 'LatPreferredLatitude'],
            'LatLatitudeDecimal': ['LatPreferredCentroidLatDec', 'LatCentroidLatitudeDec', 'LatPreferredLatitudeDec'],
            'LatLongitude': ['LatPreferredCentroidLongitude', 'LatCentroidLongitude', 'LatPreferredLongitude'],
            'LatLongitudeDecimal': ['LatPreferredCentroidLongDec', 'LatCentroidLongitudeDec', 'LatPreferredLongitudeDec']
        }

        for field_name, preferred_fields in fields.items():

            # Loop through the fields in order of preference
            for field in itertools.chain(preferred_fields, [field_name]):

                # Do we have a value
                value = data.get(field, None)

                if value:

                    # If we have multiple points, use the first one
                    if isinstance(value, list):
                        value = value[0]

                    # Set the field name value
                    data[field_name] = value

                    # Exit inner loop, continue to next field_name, pref
                    break

            # For decimal fields, convert '' to None to prevent DB error: invalid input syntax for type double precision
            if data.get(field, None) == '' and field_name in ['LatLatitudeDecimal', 'LatLongitudeDecimal']:
                data[field_name] = None

        super(SitesTask, self).process(data)
