#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '14/02/2017'.
"""

from ke2sql.models.mixin import MixinModel
from ke2sql.models.base import Base


# FIXME: Index lots??
# FIXME: Geospatial?
# FIXME: Artefacts


class EMultimediaModel(Base, MixinModel):
    """
    EMultimedia records
    """
    property_mappings = (
        # Record numbers
        ('GenDigitalMediaId', 'assetID'),
        ('MulTitle', 'title'),
        ('MulMimeFormat', 'mime'),
    )

    def is_importable(self, record):
        """
        Evaluate whether a record is importable
        At the very least a record will need AdmPublishWebNoPasswordFlag set to Y,
        Additional models will extend this to provide additional filters
        :param record:
        :return: boolean - false if not importable
        """
        # Run the record passed the base filter (checks AdmPublishWebNoPasswordFlag)
        if not super().is_importable(record):
            return False

        # Records must have a MAM asset id
        mam_asset_id = getattr(record, 'GenDigitalMediaId', None)
        if not mam_asset_id or mam_asset_id == 'Pending':
            return False

        # If it gets to the end, return True - record can be imported
        return True
