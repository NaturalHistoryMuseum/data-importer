#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '31/05/2017'.
"""

import click
import logging
import time
import json
from ke2sql.lib.solr import SolrIndex
from ke2sql.lib.config import Config
from ke2sql.lib.helpers import get_dataset_tasks


logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
logger = logging.getLogger('luigi-interface')


# Time to wait before checking the import has worked - by default 1 minute
SLEEP_INTERVAL = 2


@click.command()
@click.option('--dataset-name',  default='specimens', help='Output a SOLR schema.')
def solr_schema(dataset_name):

    for task in get_dataset_tasks():
        properties = [(f.module_name, f.field_alias) for f in task.fields if f.module_name != 'emultimedia']
        # Dedupe properties
        properties = list(set(properties))

        multimedia_sub_query = """
          (SELECT
            jsonb_agg(jsonb_build_object(
              'identifier', format('http://www.nhm.ac.uk/services/media-store/asset/%s/contents/preview', properties->>'assetID'),
              'type', 'StillImage',
              'license',  'http://creativecommons.org/licenses/by/4.0/',
              'rightsHolder',  'The Trustees of the Natural History Museum, London'
              )
            || emultimedia.properties) as "associatedMedia"
          FROM emultimedia
          WHERE emultimedia.deleted IS NULL AND (emultimedia.embargo_date IS NULL OR emultimedia.embargo_date < NOW()) AND emultimedia.irn = ANY({0}.multimedia_irns)) as mm
        """.format(task.table)

        multimedia_sub_query = """
                SELECT
        ecatalogue.irn, (SELECT
        string_agg(properties-> > 'category', '|') FROM
        emultimedia
        WHERE
        emultimedia.deleted
        IS
        NULL
        AND(emultimedia.embargo_date
        IS
        NULL
        OR
        emultimedia.embargo_date < NOW()) AND
        emultimedia.irn = ANY(ecatalogue.multimedia_irns)) as tags
        from ecatalogue where
        ecatalogue.multimedia_irns is not null;
        """.format(task.table)



        select = [
            '{0}.irn as _id'.format(task.table),
            multimedia_sub_query
        ]
        select += list(map(lambda p: '"{0}".properties->>\'{1}\' as {1}'.format(p[0], p[1]), properties))

        sql = 'SELECT {select} FROM {table} LEFT JOIN etaxonomy ON ecatalogue.indexlot_taxonomy_irn = etaxonomy.irn AND etaxonomy.deleted IS NULL WHERE ecatalogue.record_type = \'Index Lot\''.format(
            select=', '.join(select),
            table=task.table
        )


        print(sql)

if __name__ == "__main__":
    solr_schema()
