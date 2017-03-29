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
from sqlbuilder.mini import P, Q, compile

from ke2sql.lib.config import Config
from ke2sql.lib.field import Field, MetadataField
from ke2sql.lib.filter import Filter
from ke2sql.lib.ckan import ckan_get_resource, ckan_get_package, ckan_create_package
from ke2sql.lib.db import db_view_exists
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
    # Specify dry run to just create materialized views, and skip
    # Dataset creation - useful if just want to create view
    # And copy into an existing dataset
    dry_run = luigi.BoolParameter(default=False, significant=False)

    # Luigi Postgres database connections
    host = Config.get('database', 'host')
    database = Config.get('database', 'datastore_dbname')
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
        ]),
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

    def pre_query(self, connection):
        """
        Override to perform custom queries.
        """

    @property
    def query(self):
        """
        Query for building materialised view
        :return:
        """
        connection = self.output().connect()
        # Run any prepatory SQL
        self.pre_query(connection)

        # If this is a dry run, we'll use package name +view to the name so there's
        # no conflicts with existing datasets, and easy to see which is which
        view_name = self.package_name + '-view' if self.dry_run else self.resource_id

        if db_view_exists(view_name, connection):
            logger.info('Refreshing materialized view %s', self.resource_id)
            query = 'REFRESH MATERIALIZED VIEW "{view_name}"'.format(
                view_name=view_name
            )
        else:
            logger.info('Creating materialized view %s', view_name)
            query = self.get_query()
            compiled_query = compile(query)
            # print(compiled_query)
            # Use the compiled query in materialised view
            return 'CREATE MATERIALIZED VIEW "{view_name}" AS ({query})'.format(
                view_name=view_name,
                query=compiled_query[0] % tuple(compiled_query[1])
            )

        return query

    def __init__(self, *args, **kwargs):
        super(DatasetTask, self).__init__(*args, **kwargs)
        # Try and create CKAN datasets if dry run isn't set
        if not self.dry_run:
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
        # Select import class - upsert (default) or bulk copy
        if self.bulk_copy:
            # Only run bulk export if the data parameter matches the full export date
            # Otherwise there is a risk of dropping all the data, and
            # rebuilding from an update-only export
            full_export_date = Config.get('keemu', 'full_export_date')
            if full_export_date != self.date:
                raise Exception('Bulk copy import requested, but data param {date} does not match date of last full export {full_export_date}'.format(
                    date=self.date,
                    full_export_date=full_export_date
                ))
            cls = KeemuUpsertTask
        else:
            cls = KeemuCopyTask

        # Set comprehension - build set of all modules used in this dataset
        modules = list({f.module_name for f in self.fields})
        for module in modules:
            logger.info('Importing %s with %s method', module, cls)
            yield cls(module_name=module, date=self.date, limit=self.limit)

    def get_query(self):
        """
        Get query as a tuple, which can be over-ridden and altered by datasets
        :return: dict query
        """
        record_types = self.dataset_record_types()
        # Get the properties to insert - excluding emultimedia as these won't be added
        # to the properties - there are in their own jsonb collection
        properties = [(f.module_name, f.field_alias) for f in self.fields if f.module_name != 'emultimedia']
        # Dedupe properties
        properties = list(set(properties))

        # Create properties select, build list of: ecatalogue.properties->\'scientificName\'  as "scientificName"
        properties_select = list(map(lambda p: '{0}.properties->\'{1}\' as "{1}"'.format(p[0], p[1]), properties))
        # Construct record type filter
        # record_types can be list, in which case change to IN ('Type A', 'Type B')
        if type(record_types) == list:
            record_type_operator = 'IN'
            record_types = "('{0}')".format("','".join(record_types))
        else:
            record_type_operator = "="
            record_types = "'{0}'".format(record_types)

        sql = [
            'SELECT', [
                self.table + '.irn as _id',
                '(SELECT jsonb_agg(properties) from emultimedia where emultimedia.deleted is null AND emultimedia.irn = ANY(' + self.table + '.multimedia_irns)) as multimedia',
            ] + properties_select,
            'FROM', [
                self.table
            ],
            'WHERE', [
                self.table + '.record_type', record_type_operator, P(record_types), 'AND',
                self.table + '.embargo_date', 'IS', None, 'OR',
                self.table + '.embargo_date', '<', 'NOW()', 'AND',
                self.table + '.deleted', 'IS', None,
            ]
        ]

        # Use helper Q, to allow modifications through the API
        return Q(sql)

    def dataset_record_types(self):
        """
        Loop through all the filters, finding the one related to record type
        By default, all record types used in the import filter will be used to
        Build the dataset view
        :return:
        """
        for filter_ in self.filters:
            if filter_.field_name == 'ColRecordType':
                return filter_.filters[0][1]





