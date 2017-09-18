#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '14/03/2017'.

"""

import sys
import time
import logging
import abc
import luigi
from operator import is_not, ne
from prompter import yesno

from data_importer.lib.config import Config
from data_importer.lib.field import Field
from data_importer.lib.foreign_key import ForeignKeyField
from data_importer.lib.filter import Filter
from data_importer.lib.ckan import CKAN
from data_importer.tasks.solr.index import SolrIndexTask
from data_importer.lib.db import db_table_exists, db_delete_record, db_create_index
from data_importer.tasks.keemu.ecatalogue import EcatalogueTask
from luigi.contrib.postgres import CopyToTable

logger = logging.getLogger('luigi-interface')


class DatasetTask(CopyToTable):
    """
    Base Dataset Task
    """
    # KE EMu export date to process
    date = luigi.IntParameter()
    # Limit - only used when testing
    limit = luigi.IntParameter(default=None, significant=False)

    resource_type = 'csv'
    priority = 1

    # Luigi Postgres database connections
    host = Config.get('database', 'host')
    database = Config.get('database', 'datastore_dbname')
    user = Config.get('database', 'username')
    password = Config.get('database', 'password')

    # List of all fields
    fields = [
        # All datasets include create and update
        Field('ecatalogue', 'AdmDateInserted', 'created'),
        Field('ecatalogue', 'AdmDateModified', 'modified'),
        # All datasets include multimedia fields
        Field('emultimedia', 'GenDigitalMediaId', 'assetID'),
        Field('emultimedia', 'MulTitle', 'title'),
        Field('emultimedia', 'MulMimeFormat', 'mime'),
        Field('emultimedia', 'MulCreator', 'creator'),
        Field('emultimedia', 'DetResourceType', 'category'),
    ]

    # All datasets have a foreign key join with emultimedia
    foreign_keys = [
        ForeignKeyField('ecatalogue', 'emultimedia', 'MulMultiMediaRef'),
    ]

    @abc.abstractproperty
    def record_types(self):
        """
        Record type(s) to use to build this dataset
        :return: String or List
        """
        return []

    @abc.abstractproperty
    def package_name(self):
        """
        Name of the package being created
        :return: String
        """
        return None

    @abc.abstractproperty
    def resource_title(self):
        """
        Title of the resource
        :return: String
        """
        return None

    @abc.abstractproperty
    def resource_id(self):
        """
        ID of the resource
        :return: String
        """
        return None

    @property
    def table(self):
        """
        Table name - ID of the resource
        """
        return self.resource_id

    def run(self):
        self.create_ckan_dataset()
        connection = self.output().connect()
        if not db_table_exists(self.table, connection):
            self.create_table(connection)

    def create_ckan_dataset(self):
        """
        Create a dataset on CKAN
        :return:
        """

        ckan = CKAN()

        pkg_dict = {
            'name': self.package_name,
            'notes': self.package_description,
            'title': self.package_title,
            'author': Config.get('ckan', 'dataset_author'),
            'license_id': Config.get('ckan', 'dataset_licence'),
            'resources': [
                {
                    'id': self.resource_id,
                    'name': self.resource_title,
                    'description': self.resource_description,
                    'format': self.resource_type,
                    'url': '_datastore_only_resource',
                    'url_type': 'dataset'
                }
            ],
            'dataset_category': Config.get('ckan', 'dataset_type'),
            'owner_org': Config.get('ckan', 'owner_org')
        }

        package = ckan.get_package(self.package_name)
        # If we don't have a package, create it now
        if not package:
            if not yesno('Package {package_name} does not exist.  Do you want to create it?'.format(
                    package_name=self.package_name
            )):
                sys.exit("Import cancelled")

            # Check the resource doesn't exist
            resource = ckan.get_resource(self.resource_id)
            if resource:
                raise Exception('Resource {resource_title} ({resource_id}) already exists - package cannot be created')

            # Create the package
            ckan.create_package(pkg_dict)

    def create_table(self, connection):
        query = "CREATE TABLE \"{table}\" (_id INT PRIMARY KEY)".format(table=self.table)
        connection.cursor().execute(query)
        connection.commit()

    def requires(self):
        """
        Luigi requires
        Just requires the keemu ecatalogue import - which is dependent
        On etaxonomy and emultimedia
        :return:
        """
        yield (
            EcatalogueTask(date=self.date, limit=self.limit),
            SolrIndexTask(core=self.package_name)
        )
