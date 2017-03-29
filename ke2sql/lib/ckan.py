#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '21/03/2017'.
"""

import ckanapi
import psycopg2
from ke2sql.lib.config import Config





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


def ckan_get_api_key():
    connection = psycopg2.connect(
        host=Config.get('database', 'host'),
        port=Config.get('database', 'port'),
        database=Config.get('database', 'ckan_dbname'),
        user=Config.get('database', 'username'),
        password=Config.get('database', 'password')
    )
    cursor = connection.cursor()
    sql = """ SELECT apikey
              FROM public.user
              WHERE name = %s;
          """
    cursor.execute(sql, (Config.get('ckan', 'api_user'),))
    result = cursor.fetchone()
    return result[0]


remote_ckan = ckanapi.RemoteCKAN(Config.get('ckan', 'site_url'), apikey=ckan_get_api_key())
