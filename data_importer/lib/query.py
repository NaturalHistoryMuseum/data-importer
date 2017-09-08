#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '22/03/2017'.
"""


class Query(object):

    def __init__(self, select):
        self.select = select

    def to_sql(self):
        return ""
