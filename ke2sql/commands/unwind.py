#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '04/04/2017'.
"""

import re
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

        # New name of the view
        view_new_name = '{0}-view'.format(task.resource_id)

        # Check if the renamed view already exists
        # If it doesn't, we want to rename the resource to use the renamed view
        if not db_view_exists(view_new_name, connection):
            # Renamed view does not exist - try and rename it now
            if not db_view_exists(task.resource_id, connection):
                raise Exception('Base resource id %s does not exist' % task.resource_id)
            print('Renaming base view %s' % task.resource_id)
            cursor.execute('ALTER MATERIALIZED VIEW "{resource_id}" RENAME TO "{view_new_name}"'.format(
                resource_id=task.resource_id,
                view_new_name=view_new_name
            ))

        sql = get_unwind_sql(task, table_name=task.resource_id, view_name=view_new_name)

        # If we have no sql, just continue
        if not sql:
            continue

        # If the table already exists, drop it
        cursor.execute('DROP TABLE IF EXISTS "{table_name}"'.format(
            table_name=task.resource_id
        ))
        # Execute the SQL to create another table
        # print(sql)
        cursor.execute(sql)

        # Add some indexes
        indexes = [
            'ALTER TABLE "{table_name}" ADD PRIMARY KEY (_id)'.format(
                table_name=task.resource_id
            )
        ]
        for indexed_field in get_indexed_fields(task):
            indexes.append('CREATE INDEX ON "{table_name}"("{field_name}")'.format(
                table_name=task.resource_id,
                field_name=indexed_field
            ))

        # Execute indexes SQL
        cursor.execute(';'.join(indexes))

    connection.commit()
    connection.close()


def get_unwind_sql(task, table_name, view_name):
    """
    Get unwinding SQL for a particular task
    """
    # Get the properties to insert - excluding emultimedia as these won't be added
    # to the properties - there are in their own jsonb collection
    properties = [(f.module_name, f.field_alias) for f in task.fields if f.module_name != 'emultimedia']
    # Dedupe properties
    properties = list(set(properties))

    sql = None

    # if task.resource_id == '05ff2255-c38a-40c9-b657-4ccb55ab2feb':
    #     # Collection code is truncated
    #     properties.remove(('ecatalogue', 'collectionCode'))
    #     properties.remove(('ecatalogue', 'dateModified'))
    #     properties.remove(('ecatalogue', 'dateCreated'))
    #     properties.remove(('ecatalogue', 'decimalLongitude'))
    #     properties.remove(('ecatalogue', 'decimalLatitude'))
    #     properties.remove(('ecatalogue', 'occurrenceID'))
    #
    #     properties.extend([
    #         ('ecatalogue', 'basisOfRecord'),
    #         ('ecatalogue', 'institutionCode'),
    #         ('ecatalogue', 'otherCatalogNumbers'),
    #         ('ecatalogue', 'collectionCode')
    #     ])
    #
    #     properties_select = list(map(lambda p: 'CAST("{0}".properties->>\'{1}\' AS TEXT) as "{1}"'.format(view_name, p[1]), properties))
    #
    #     ts_index = list(map(lambda p: 'to_tsvector(\'english\', coalesce("{0}".properties->>\'{1}\', \'\'))'.format(view_name, p[1]), properties))
    #
    #     sql = """CREATE TABLE "{table_name}" AS (
    #         SELECT
    #             "{view_name}"._id,
    #             cast("{view_name}".properties->>'occurrenceID' AS UUID) as "occurrenceID",
    #             "{view_name}"."associatedMedia"::text,
    #             "{view_name}".properties->>'dateCreated' as created,
    #             "{view_name}".properties->>'dateModified' as modified,
    #             cast("{view_name}".properties->>'decimalLongitude' AS FLOAT8) as "decimalLongitude",
    #             cast("{view_name}".properties->>'decimalLatitude' AS FLOAT8) as "decimalLatitude",
    #             "{view_name}"._geom,
    #             "{view_name}"._the_geom_webmercator,
    #             'Specimen'::TEXT as "recordType",
    #             {properties_select},
    #             NULL::TEXT as "maxError",
    #             NULL::TEXT as "cultivated",
    #             NULL::nested as "determinations",
    #             NULL::TEXT as "relationshipOfResource",
    #             NULL::TEXT as "relatedResourceID",
    #             FALSE as centroid,
    #             {ts_index} as _full_text
    #             FROM "{view_name}"
    #             WHERE "{view_name}".properties->>'occurrenceID' != ')')
    #     """.format(
    #         table_name=table_name,
    #         view_name=view_name,
    #         properties_select=','.join(properties_select),
    #         ts_index=' || '.join(ts_index)
    #     )
    # elif task.resource_id == 'bb909597-dedf-427d-8c04-4c02b3a24db3':
    #
    #     original_fields = [
    #         'Currently accepted name',
    #         'Original name',
    #         'Kingdom',
    #         'Phylum',
    #         'Class',
    #         'Order',
    #         'Suborder',
    #         'Superfamily',
    #         'Family',
    #         'Subfamily',
    #         'Genus',
    #         'Subgenus',
    #         'Species',
    #         'Subspecies',
    #         'Taxonomic rank',
    #         'GUID',
    #         'IRN',
    #         'Material',
    #         'Type',
    #         'Media',
    #         'British',
    #         'Kind of material',
    #         'Kind of media',
    #         'Material count',
    #         'Material sex',
    #         'Material stage',
    #         'Material types',
    #         'Material primary type no',
    #         # 'Department',
    #         # 'Modified',
    #         # 'Created'
    #     ]
    #
    #     property_mappings = {p[1]: camel_case_to_sentence(p[1]) for p in properties}
    #     # Manual override
    #     property_mappings['currentScientificName'] = 'Currently accepted name'
    #     property_mappings['scientificName'] = 'Original name'
    #     property_mappings['specificEpithet'] = 'Species'
    #     property_mappings['infraspecificEpithet'] = 'Subspecies'
    #     property_mappings['taxonRank'] = 'Taxonomic rank'
    #     property_mappings['GUID'] = 'GUID'
    #     property_mappings['_id'] = 'IRN'
    #     property_mappings['materialPrimaryTypeNumber'] = 'Material primary type no'
    #
    #     new_fields = list(property_mappings.values())
    #
    #     # Ensure all the fields exist
    #     for original_field in original_fields:
    #         try:
    #             new_fields.remove(original_field)
    #         except ValueError:
    #             print(original_field)
    #             raise
    #
    #     if new_fields:
    #         raise Exception('Extra fields')
    #
    #     properties_select = ['CAST("{0}".properties->>\'{1}\' AS CITEXT) as "{2}"'.format(view_name, x, y) for x,y in property_mappings.items()]
    #
    #     sql = """CREATE TABLE "{table_name}" AS (
    #         SELECT
    #             "{view_name}"._id,
    #             "{view_name}"."multimedia"::text,
    #             "{view_name}".properties->>'dateCreated' as created,
    #             "{view_name}".properties->>'dateModified' as modified,
    #             {properties_select},
    #               ''::tsvector as _full_text
    #             FROM "{view_name}")
    #     """.format(
    #         table_name=table_name,
    #         view_name=view_name,
    #         properties_select=','.join(properties_select)
    #     )

    if task.resource_id == 'ec61d82a-748d-4b53-8e99-3e708e76bc4d':
        properties_select = list(map(lambda p: 'CAST("{0}".properties->>\'{1}\' AS TEXT) as "{1}"'.format(view_name, p[1]), properties))
        sql = """CREATE TABLE "{table_name}" AS (
                SELECT
                    "{view_name}"._id,
                    "{view_name}"."multimedia"::text,
                    "{view_name}".properties->>'dateCreated' as created,
                    "{view_name}".properties->>'dateModified' as modified,
                    {properties_select},
                      ''::tsvector as _full_text
                    FROM "{view_name}")
            """.format(
            table_name=table_name,
            view_name=view_name,
            properties_select=','.join(properties_select)
        )

    return sql


def camel_case_to_sentence(camel_case_str):
    """
    Convert camel case string to sentence
    @param camel_case_str:
    @return:
    """
    return re.sub(r'(?<=[a-zA-Z])(?=[A-Z])', ' ', camel_case_str).capitalize()


def get_indexed_fields(task):
    if task.resource_id == '05ff2255-c38a-40c9-b657-4ccb55ab2feb':
        return ['collectionCode', 'catalogNumber', 'created', 'project']
    return []

if __name__ == "__main__":
    unwind()
