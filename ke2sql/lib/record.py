#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '14/02/2017'.
"""


class Record(object):
    """
    Object with setter overridden so multi-value fields are turned into an array
    """
    def __setattr__(self, key, value):
        if key in self.__dict__:
            try:
                self.__dict__[key].append(value)
            except AttributeError:
                self.__dict__[key] = [self.__dict__[key], value]
        else:
            self.__dict__[key] = value

    def to_dict(self, property_mappings):
        """
        Method to build a dict of properties
        :param property_mappings:
        :return:
        """
        return {alias: getattr(self, field, None) for (field, alias) in property_mappings if getattr(self, field, None)}
