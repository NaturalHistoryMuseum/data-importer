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


from ke2sql.lib.config import Config
from ke2sql.lib.field import Field, MetadataField
from ke2sql.lib.filter import Filter
from ke2sql.lib.ckan import CKAN
from ke2sql.lib.helpers import list_all_modules
from ke2sql.tasks.keemu import KeemuCopyTask, KeemuUpsertTask
from ke2sql.tasks.postgres.view import MaterialisedViewTask


logger = logging.getLogger('luigi-interface')


class DatasetTask(MaterialisedViewTask):
    """
    Base Dataset Task
    """
    date = luigi.IntParameter()
    # Limit - only used when testing
    limit = luigi.IntParameter(default=None, significant=False)

    # Specify view only to just create materialized views, and skip
    # Dataset creation - useful if just want to create view
    # And copy into an existing dataset
    view_only = luigi.BoolParameter(default=False, significant=False)

    # Luigi Postgres database connections
    host = Config.get('database', 'host')
    database = Config.get('database', 'datastore_dbname')
    user = Config.get('database', 'username')
    password = Config.get('database', 'password')
    table = 'ecatalogue'
    resource_type = 'csv'

    # List of all fields, as tuples:
    #     (KE EMu field, Dataset field)
    fields = [
        # All datasets include multimedia fields
        Field('emultimedia', 'GenDigitalMediaId', 'assetID'),
        Field('emultimedia', 'MulTitle', 'title'),
        Field('emultimedia', 'MulMimeFormat', 'mime'),
        Field('emultimedia', 'MulCreator', 'creator'),
    ]

    metadata_fields = [
        # All datasets will populate record type
        MetadataField("ecatalogue", "ColRecordType", "record_type", "TEXT"),
        # Populate embargo date
        # Will use NhmSecEmbargoExtensionDate if set; otherwise NhmSecEmbargoDate
        MetadataField('ecatalogue', 'NhmSecEmbargoDate', 'embargo_date', "DATE"),
        MetadataField('ecatalogue', 'NhmSecEmbargoExtensionDate', 'embargo_date', "DATE"),
        MetadataField('emultimedia', 'NhmSecEmbargoDate', 'embargo_date', "DATE"),
        MetadataField('emultimedia', 'NhmSecEmbargoExtensionDate', 'embargo_date', "DATE"),
    ]

    # List of filters to apply to build this dataset
    filters = [
        # All datasets include multimedia records, which are filtered on having a MAM Asset ID
        Filter('emultimedia', 'GenDigitalMediaId', [
            (is_not, None),
            (ne, 'Pending')
        ]),
    ]

    # Query for building multimedia json
    multimedia_sub_query = """
      SELECT
        jsonb_agg(jsonb_build_object(
          'identifier', format('http://www.nhm.ac.uk/services/media-store/asset/%s/contents/preview', properties->>'assetID'),
          'type', 'StillImage',
          'license',  'http://creativecommons.org/licenses/by/4.0/',
          'rightsHolder',  'The Trustees of the Natural History Museum, London'
          )
        || emultimedia.properties) as "associatedMedia"
      FROM emultimedia
      WHERE emultimedia.deleted IS NULL AND (emultimedia.embargo_date IS NULL OR emultimedia.embargo_date < NOW()) AND emultimedia.irn = ANY(cat.multimedia_irns)
    """

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

    @abc.abstractproperty
    def sql(self):
        """
        SQL to build the dataset
        Just a raw string, does duplicate some logic (where statements etc.,)
        But is so much more readable and easier to see what's happening
        :return: String
        """
        return None

    @property
    def views(self):
        """
        MaterialisedViewTask.views - return (name, sql) tuple to define materialised view
        :return:
        """
        return [
            (self.resource_id, self.sql)
        ]

    def __init__(self, *args, **kwargs):
        super(DatasetTask, self).__init__(*args, **kwargs)
        # Try and create CKAN datasets if view_only isn't set
        if not self.view_only:
            self.create_ckan_dataset()

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

    def requires(self):
        """
        Luigi requires
        Dynamically build task dependencies, for reading in KE EMu files
        And writing to the database
        :return:
        """
        full_export_date = Config.getint('keemu', 'full_export_date')
        # If this is the full export date, then use the bulk copy class
        cls = KeemuCopyTask if full_export_date == self.date else KeemuUpsertTask
        # Set comprehension - build set of all modules used in this dataset
        for module in list_all_modules():
            logger.info('Importing %s with %s method', module, cls)
            yield cls(module_name=module, date=self.date, limit=self.limit)
        return []
