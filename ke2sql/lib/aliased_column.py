#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '01/03/2017'.
"""

from sqlalchemy import Column


class AliasedColumn(Column):
    """
    SQLAlchemy Column with an alias, for denoting KE EMu name
    """
    def __init__(self, *args, **kwargs):
        # Set the column alias
        self.alias = kwargs.pop('alias')
        super(AliasedColumn, self).__init__(*args, **kwargs)
