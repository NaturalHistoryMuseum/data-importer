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
from luigi.contrib.postgres import PostgresQuery
from prompter import yesno

from ke2sql.lib.config import Config
from ke2sql.lib.field import Field, MetadataField
from ke2sql.lib.filter import Filter
from ke2sql.lib.ckan import ckan_get_resource, ckan_get_package, ckan_create_package
from ke2sql.tasks.keemu import KeemuCopyTask, KeemuUpsertTask


logger = logging.getLogger('luigi-interface')


class DatasetTask(PostgresQuery):
    """
    Base Dataset Task
    """
    date = luigi.IntParameter()
    # Limit - only used when testing
    limit = luigi.IntParameter(default=None, significant=False)
    # Import method - copy or upsert
    bulk_copy = luigi.BoolParameter(default=False, significant=False)

    # Luigi Postgres database connections
    host = Config.get('database', 'host')
    database = Config.get('database', 'database')
    user = Config.get('database', 'username')
    password = Config.get('database', 'password')

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
    ]

    # List of filters to apply to build this dataset
    filters = [
        # All datasets include multimedia records, which are filtered on having a MAM Asset ID
        Filter('emultimedia', 'GenDigitalMediaId', [
            (is_not, None),
            (ne, 'Pending')
        ])
    ]

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
    def update_id(self):
        """
        Ensure update ID is always a unique identifier
        So tasks always runs, and there's no insert conflicts on touch()
        """
        return '{0}__{1}'.format(
            self.task_id,
            time.time()
        )

    @property
    def table(self):
        """
        Base table is always ecatalogue
        """
        return 'ecatalogue'

    @property
    def query(self):
        """
        Query for building materialised view
        :return:
        """

        # FIXME: BUILD FROM PARTS - select, from, where


        return 'SELECT 1'

        connection = self.output().connect()
        if self.view_exists(self.dataset_id, connection):
            logger.info('Refreshing materialized view %s', self.dataset_id)
            query = "REFRESH MATERIALIZED VIEW {dataset_id}".format(
                dataset_id=self.dataset_id
            )
        else:
            logger.info('Creating materialized view %s', self.dataset_id)
            query = """
                CREATE MATERIALIZED VIEW {dataset_id} AS
                (SELECT {table}.irn as _id,
                (SELECT jsonb_agg(properties) from emultimedia where emultimedia.deleted is null AND emultimedia.irn = ANY({table}.multimedia_irns)) as multimedia,
                {properties} from {table}
                     LEFT JOIN etaxonomy ON etaxonomy.irn = {table}.indexlot_taxonomy_irn
                WHERE record_type = '{record_type}' and (embargo_date is null or embargo_date < now()) and {table}.deleted is null)
                """.format(
                record_type=self.record_type,
                dataset_id=self.dataset_id,
                properties=','.join(map(lambda p: '{0}.properties->\'{1}\' as "{1}"'.format(p[0], p[1]), self._dest_properties)),
                table=self.table
            )
        return query

    def __init__(self, *args, **kwargs):
        super(DatasetTask, self).__init__(*args, **kwargs)
        self.create_ckan_dataset()

    def create_ckan_dataset(self):
        """
        Create a dataset on CKAN
        :return:
        """
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

        package = ckan_get_package(self.package_name)
        # If we don't have a package, create it now
        if not package:
            if not yesno('Package {package_name} does not exist.  Do you want to create it?'.format(
                    package_name=self.package_name
            )):
                sys.exit("Import cancelled")

            # Check the resource doesn't exist
            resource = ckan_get_resource(self.resource_id)
            if resource:
                raise Exception('Resource {resource_title} ({resource_id}) already exists - package cannot be created')

            # Create the package
            ckan_create_package(pkg_dict)

    def requires(self):
        # Set comprehension - build set of all modules used in this dataset
        modules = list({f.module_name for f in self.fields})
        cls = KeemuCopyTask if self.bulk_copy else KeemuUpsertTask
        for module in modules:
            logger.info('Importing %s with %s method', module, cls)
            yield cls(module_name=module, date=self.date, limit=self.limit)





