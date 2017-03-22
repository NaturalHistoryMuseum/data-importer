#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '14/03/2017'.



"""

import time
import logging
import abc
import luigi
from operator import is_not, ne
from luigi.contrib.postgres import PostgresQuery

from ke2sql.lib.config import Config
from ke2sql.tasks.keemu import KeemuCopyTask, KeemuUpsertTask

logger = logging.getLogger('luigi-interface')


class DatasetTask(PostgresQuery):
    """
    Base Dataset Task
    """
    date = luigi.IntParameter()
    # Limit - only used when testing
    limit = luigi.IntParameter(default=None, significant=False)

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
        ('emultimedia.GenDigitalMediaId', 'assetID'),
        ('emultimedia.MulTitle', 'title'),
        ('emultimedia.MulMimeFormat', 'mime'),
        ('emultimedia.MulCreator', 'creator'),
    ]

    # List of filters to apply to build this dataset
    filters = {
        # All datasets include multimedia records, which are filtered on having a MAM Asset ID
        'emultimedia.GenDigitalMediaId': [
            (is_not, None),
            (ne, 'Pending')
        ]
    }

    @abc.abstractproperty
    def package_name(self):
        """
        Name of the package being created
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



    # def __init__(self, *args, **kwargs):
    #     super(BaseDatasetTask, self).__init__(*args, **kwargs)
        # Get the dataset ID - or create dataset if it doesn't already exist

        # resource = self.get_or_create_resource()

        # resource = {
        #     'id': 'indexlot1'
        # }
        #
        # _src_properties = [tuple(src_prop.split('.')) for src_prop, _ in self.properties]
        # # Create list of properties tuple, consisting of module name and destination field name
        # self._dest_properties = [tuple([src_prop.split('.')[0], dest_prop]) for src_prop, dest_prop in self.properties]
        #
        # self.dataset_id = self.package_name.replace('-', '')

    def requires(self):
        # Set comprehension - build set of all modules used in this dataset
        modules = list({f[0].split('.')[0] for f in self.fields})
        for module in modules:
            if module != 'ecatalogue':
                continue
            yield KeemuCopyTask(module_name=module, date=self.date)





