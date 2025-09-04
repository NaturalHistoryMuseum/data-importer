# !/usr/bin/env python
# encoding: utf-8

from typing import Tuple

from dataimporter.lib.view import SUCCESS_RESULT, FilterResult


def is_member_error(
    filter_error: FilterResult,
) -> Tuple[FilterResult, FilterResult, FilterResult]:
    """
    Get a tuple of filter responses that would be returned from is_member,
    is_publishable, and is_publishable_member respectively, where is_member does not
    return a success result. Convenience function used in
    is_publishable_member_scenarios.

    :param filter_error: the error returned by is_member
    :return: a tuple of filter results where the first and last are the same
    """
    return filter_error, SUCCESS_RESULT, filter_error


def is_publishable_error(
    filter_error: FilterResult,
) -> Tuple[FilterResult, FilterResult, FilterResult]:
    """
    Get a tuple of filter responses that would be returned from is_member,
    is_publishable, and is_publishable_member respectively, where is_publishable does
    not  return a success result. Convenience function used in
    is_publishable_member_scenarios.

    :param filter_error: the error returned by is_publishable
    :return: a tuple of filter results where the second and last are the same
    """
    return SUCCESS_RESULT, filter_error, filter_error
