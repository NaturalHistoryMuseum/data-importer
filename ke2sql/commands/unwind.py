#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '04/04/2017'.
"""

import click
import psycopg2

from ke2sql.lib.config import Config
from ke2sql.lib.helpers import get_dataset_tasks
from ke2sql.lib.db import db_view_exists


@click.command()
def unwind():
    """
    Convert the new style dataset with jsonp properties into the old wide & sparse dataset
    :return:
    """

    connection = psycopg2.connect(
        host=Config.get('database', 'host'),
        port=Config.get('database', 'port'),
        database=Config.get('database', 'datastore_dbname'),
        user=Config.get('database', 'username'),
        password=Config.get('database', 'password')
    )
    cursor = connection.cursor()

    for task in get_dataset_tasks():

        print('Unwinding %s' % task)

        if task.resource_id != '05ff2255-c38a-40c9-b657-4ccb55ab2feb':
            continue

        # If the base view doesn't exist, continue to next task
        if not db_view_exists(task.resource_id, connection):
            continue

        table_name = "{package_name}".format(
            package_name=task.package_name,
        )

        cursor.execute('DROP TABLE IF EXISTS "{table_name}"'.format(
            table_name=table_name
        ))

        # Get the properties to insert - excluding emultimedia as these won't be added
        # to the properties - there are in their own jsonb collection
        properties = [(f.module_name, f.field_alias) for f in task.fields if f.module_name != 'emultimedia']
        # Dedupe properties
        properties = list(set(properties))

        # Collection code is truncated
        properties.remove(('ecatalogue', 'collectionCode'))
        properties.remove(('ecatalogue', 'dateModified'))
        properties.remove(('ecatalogue', 'dateCreated'))
        properties.remove(('ecatalogue', 'decimalLongitude'))
        properties.remove(('ecatalogue', 'decimalLatitude'))
        properties.remove(('ecatalogue', 'occurrenceID'))

        properties.extend([
            ('ecatalogue', 'basisOfRecord'),
            ('ecatalogue', 'institutionCode'),
            ('ecatalogue', 'otherCatalogNumbers'),
            ('ecatalogue', 'collectionCode')
        ])

        indexed_fields = ['collectionCode', 'catalogNumber', 'created', 'project']

        properties_select = list(map(lambda p: 'CAST("{0}".properties->>\'{1}\' AS CITEXT) as "{1}"'.format(task.resource_id, p[1]), properties))

        sql = """CREATE TABLE "{table_name}" AS (
            SELECT
                "{resource_id}"._id,
                cast("{resource_id}".properties->>'occurrenceID' AS UUID) as "occurrenceID",
                "{resource_id}"."associatedMedia"::text,
                "{resource_id}".properties->>'dateCreated' as created,
                "{resource_id}".properties->>'dateModified' as modified,
                cast("{resource_id}".properties->>'decimalLongitude' AS FLOAT8) as "decimalLongitude",
                cast("{resource_id}".properties->>'decimalLatitude' AS FLOAT8) as "decimalLatitude",
                "{resource_id}"._geom,
                "{resource_id}"._the_geom_webmercator,
                'Specimen' as "recordType",
                {properties_select},
                NULL::TEXT as "maxError",
                NULL::nested as "determinations",
                NULL::TEXT as "relationshipOfResource",
                NULL::TEXT as "relatedResourceID",
                FALSE as centroid,
                  ''::tsvector as _full_text
                FROM "{resource_id}"
                WHERE "{resource_id}".properties->>'occurrenceID' != ')')
        """.format(
            table_name=table_name,
            resource_id=task.resource_id,
            properties_select=','.join(properties_select)
        )
        cursor.execute(sql)
        indexes = [
            'ALTER TABLE "{table_name}" ADD PRIMARY KEY (_id)'.format(
                table_name=table_name
            )
        ]
        for indexed_field in indexed_fields:
            indexes.append('CREATE INDEX ON "{table_name}"("{field_name}")'.format(
                table_name=table_name,
                field_name=indexed_field
            ))

        # Execute indexes SQL
        cursor.execute(';'.join(indexes))

    connection.commit()

if __name__ == "__main__":
    unwind()
