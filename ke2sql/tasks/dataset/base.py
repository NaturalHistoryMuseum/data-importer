#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '14/03/2017'.



"""

import time
import logging
import ckanapi
from prompter import prompt, yesno
from luigi.contrib.postgres import PostgresQuery
from abc import abstractproperty

from ke2sql.lib.config import Config


logger = logging.getLogger('luigi-interface')


class BaseDatasetTask(PostgresQuery):
    """
    Base Dataset Task
    """

    # Luigi Postgres database connections
    host = Config.get('database', 'host')
    database = Config.get('database', 'database')
    user = Config.get('database', 'username')
    password = Config.get('database', 'password')
    table = 'ecatalogue'

    def __init__(self, *args, **kwargs):
        super(BaseDatasetTask, self).__init__(*args, **kwargs)
        # Get the dataset ID - or create dataset if it doesn't already exist
        self.remote_ckan = ckanapi.RemoteCKAN(Config.get('ckan', 'site_url'), apikey=Config.get('ckan', 'api_key'))
        # FIXME: Get or create a dataset
        # resource = self.get_or_create_resource()
        resource = {
            'id': 'indexlot1'
        }

        _src_properties = [tuple(src_prop.split('.')) for src_prop, _ in self.properties]
        # Create list of properties tuple, consisting of module name and destination field name
        self._dest_properties = [tuple([src_prop.split('.')[0], dest_prop]) for src_prop, dest_prop in self.properties]

        self.dataset_id = self.package_name.replace('-', '')

    @abstractproperty
    def properties(self):
        """
        List of properties to use from the properties jsonp
        NB: These will not be needed for long
        When using JSON, it should only hold properties
        relating to that object
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
    def query(self):
        """
        Table name - lower case class name up to third capital
        ECatalogueCopyTask => ecatalogue
        ECatalogueUpdateTask => ecatalogue
        ECatalogueTask => ecatalogue
        :return:
        """
        connection = self.output().connect()
        if self.view_exists(connection):
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

    def view_exists(self, connection):
        cursor = connection.cursor()
        cursor.execute("select exists(select * from pg_matviews where matviewname=%s)", (self.dataset_id,))
        return cursor.fetchone()[0]

    def get_or_create_resource(self):
        """
        Either load a resource object
        Or if it doesn't exist, prompt user to see if they want to create the dataset package, and datastore
        @param package: params to create the package
        @param datastore: params to create the datastore
        @return: CKAN resource ID
        """

        resource_id = None

        try:
            # If the package exists, retrieve the resource
            ckan_package = self.remote_ckan.action.package_show(id=self.package_name)
        except ckanapi.NotFound:
            if yesno('Dataset {package_name} does not exist. Do you want to create it now?'.format(
                    package_name=self.package_name
            )):
                # FIXME: Add alias
                ckan_package = self.remote_ckan.action.package_create({
                    'name': self.package_name,
                    'notes': self.package_description,
                    'title': self.package_title,
                    'author': Config.get('ckan', 'dataset_author'),
                    'license_id': Config.get('ckan', 'dataset_licence'),
                    'resources': [],
                    'dataset_category': Config.get('ckan', 'dataset_type'),
                    'owner_org': Config.get('ckan', 'owner_org')
                })

        # # If we don't have the resource ID, create
        # if not resource_id:
        #     log.info("Resource %s not found - creating", self.datastore['resource']['name'])
        #
        #     self.datastore['fields'] = [{'id': col, 'type': self.numpy_to_ckan_type(np_type)} for col, np_type in self.get_output_columns().iteritems()]
        #     self.datastore['resource']['package_id'] = ckan_package['id']
        #
        #     if self.indexed_fields:
        #         # Create BTREE indexes for all specified indexed fields
        #         self.datastore['indexes'] = [col['id'] for col in self.datastore['fields'] if col['id'] in self.indexed_fields]
        #     else:
        #         # Create BTREE indexes for all citext fields
        #         self.datastore['indexes'] = [col['id'] for col in self.datastore['fields'] if col['type'] == 'citext']
        #
        #     # API call to create the datastore
        #     resource_id = self.remote_ckan.action.datastore_create(**self.datastore)['resource_id']
        #
        #     # If this has geospatial fields, create geom columns
        #     if self.geospatial_fields:
        #         log.info("Creating geometry columns for %s", resource_id)
        #         self.geospatial_fields['resource_id'] = resource_id
        #         self.remote_ckan.action.create_geom_columns(**self.geospatial_fields)
        #
        #     log.info("Created datastore resource %s", resource_id)

        return resource_id