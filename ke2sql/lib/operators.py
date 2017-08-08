#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '03/03/2017'.
"""
from uuid import UUID

def is_one_of(a, b):
    """
    Helper operator a is in list b
    :param a: str
    :param b: list
    :return: boolean
    """
    return a in b


def is_not_one_of(a, b):
    """
    Helper operator a is not in list b
    :param a: str
    :param b: list
    :return: boolean
    """
    return a not in b


def is_uuid(a):
    """
    Helper operator - check value a is valid uuid
    :param a: uuid
    :return: boolean
    """
    try:
        UUID(a)
    except ValueError:
        # If it's a value error, then the string
        # is not a valid hex code for a UUID.
        return False
    return True
