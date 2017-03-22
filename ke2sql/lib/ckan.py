#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '21/03/2017'.
"""

import ckanapi
from ke2sql.lib.config import Config


remote_ckan = ckanapi.RemoteCKAN(Config.get('ckan', 'site_url'), apikey=Config.get('ckan', 'api_key'))


def ckan_get_package(package_name):
    """
    Get a resource id for a dataset dict
    Resource ID is also use as datastore table name
    :param package_name:
    :return: resource id
    """
    try:
        return remote_ckan.action.package_show(id=package_name)
    except ckanapi.NotFound:
        return False


def ckan_get_resource(resource_id):
    """
    Get a resource id for a dataset dict
    Resource ID is also use as datastore table name
    :param resource_id:
    :return: resource id
    """
    try:
        return remote_ckan.action.resource_show(id=resource_id)
    except ckanapi.NotFound:
        return False


def ckan_create_package(pkg_dict):
    remote_ckan.action.package_create(**pkg_dict)
