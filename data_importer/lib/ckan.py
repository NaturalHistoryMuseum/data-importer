#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '21/03/2017'.
"""

import ckanapi
import psycopg2
from data_importer.lib.config import Config


class CKAN(object):
    """
    Interact with CKAN instance via API
    """
    def __init__(self):
        self.remote_ckan = ckanapi.RemoteCKAN(Config.get('ckan', 'site_url'), apikey=self.get_api_key())

    @staticmethod
    def get_api_key():
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

    def get_package(self, package_name):
        """
        Get a resource id for a dataset dict
        Resource ID is also use as datastore table name
        :param package_name:
        :return: resource id
        """
        try:
            return self.remote_ckan.action.package_show(id=package_name)
        except ckanapi.NotFound:
            return False

    def create_package(self, pkg_dict):
        self.remote_ckan.action.package_create(**pkg_dict)

    def get_resource(self, resource_id):
        """
        Get a resource id for a dataset dict
        Resource ID is also use as datastore table name
        :param resource_id:
        :return: resource id
        """
        try:
            return self.remote_ckan.action.resource_show(id=resource_id)
        except ckanapi.NotFound:
            return False

    def update_resource(self, resource_dict):
        self.remote_ckan.action.resource_update(**resource_dict)
