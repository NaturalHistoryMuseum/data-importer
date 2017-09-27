#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '04/09/2017'.
"""

import click
from data_importer.lib.dataset import dataset_get_tasks
from collections import OrderedDict


class SolrCommand(object):
    def __init__(self, dataset_name):
        self.dataset_name = dataset_name
        self.dataset = self._get_dataset(dataset_name)

        # Extra field names
        if self.dataset_name == 'collection-specimens':
            self.multimedia_field = 'associatedMedia'
            self.guid_field = 'occurrenceID'
        else:
            self.multimedia_field = 'multimedia'
            self.guid_field = 'GUID'

    @staticmethod
    def _get_dataset(dataset_name):
        """
        Get dataset matching the package name
        @return:
        """
        for dataset in dataset_get_tasks():
            if dataset_name == dataset.package_name:
                return dataset

        # If we haven't found a dataset, raise an error
        names = [d.package_name for d in dataset_get_tasks()]
        raise click.UsageError('Unknown dataset name - allowed values are: %s' % ' '.join(names))

    @property
    def dataset_fields(self):
        fields = set()
        for field in self.dataset.fields:
            # Multimedia and created/modified are handled separately - created/modified
            # because they're dates; multimedia because it's jsonb
            if field.module_name is 'emultimedia' or field.field_alias in ['created', 'modified']:
                continue
            fields.add(field)
        return fields

    # Commands
    def get_sql(self):
        primary_table = 'ecatalogue'
        multimedia_view = '_multimedia_view'

        # We don't want to include mammal group parent in the output - these
        # records
        try:
            self.dataset.record_types.remove('Mammal Group Parent')
        except ValueError:
            pass

        sql = OrderedDict([
            ('SELECT', [
                '{}.irn as _id'.format(primary_table),
                '{0}.guid::text as {1}'.format(primary_table, self.guid_field),
                # Format created and modified so they're acceptable dates for SOLR
                '{}.properties->>\'created\' || \'T00:00:00Z\' as created'.format(primary_table),
                '{}.properties->>\'modified\' || \'T00:00:00Z\' as modified'.format(primary_table),
                '{0}.multimedia as {1}'.format(multimedia_view, self.multimedia_field),
                '{}.category as imageCategory'.format(multimedia_view),
                'CASE WHEN {}.irn IS NULL THEN false ELSE true END AS _has_multimedia'.format(multimedia_view),
            ]),
            ('FROM', [primary_table]),
            ('WHERE', [
                '{0}.record_type IN (\'{1}\')'.format(
                    primary_table,
                    '\',\''.join(self.dataset.record_types)
                ),
                '({0}.embargo_date IS NULL OR {0}.embargo_date < NOW())'.format(primary_table),
                '{0}.deleted IS NULL'.format(primary_table)
            ]),
        ])

        # Fields to be coallesced in the specimens - join by parent catalogue properties
        coalesced_fields = [
            'class',
            'genus',
            'family',
            'phylum',
            'kingdom',
            'order',
            'scientificName',
            'specificEpithet',
            'sex',
            'locality'
        ]

        for field in self.dataset_fields:
            # Blank recordedBy and identifiedBy fields
            if field.field_alias in ['recordedBy', 'identifiedBy']:
                sql['SELECT'].append('NULL as "{0}"'.format(
                    field.field_alias
                ))
            elif self.dataset_name == 'collection-specimens' and field.field_alias in coalesced_fields:
                # TODO: Check which modules these fields are from - e.g etaxonomy or ecatalogue
                sql['SELECT'].append('COALESCE({0}.properties->>\'{1}\', parent_catalogue.properties->>\'{1}\', etaxonomy.properties->>\'{1}\') as "{1}"'.format(
                    field.module_name,
                    field.field_alias
                ))
            else:
                sql['SELECT'].append('{0}.properties->>\'{1}\' as "{1}"'.format(
                    field.module_name,
                    field.field_alias
                ))

        for fk in self.dataset.foreign_keys:

            # Special handling for multimedia
            if fk.join_module == 'emultimedia':
                sql['FROM'].append('LEFT JOIN _multimedia_view ON _multimedia_view.irn={primary_table}.irn'.format(
                    primary_table=primary_table,
                ))
            else:
                if fk.record_type:
                    sql['FROM'].append('LEFT JOIN {fk_table} ON {fk_table}.irn={primary_table}.irn AND {primary_table}.record_type=\'{record_type}\''.format(
                        fk_table=fk.table,
                        primary_table=primary_table,
                        record_type=fk.record_type
                    ))
                else:
                    sql['FROM'].append('LEFT JOIN {fk_table} ON {fk_table}.irn={primary_table}.irn'.format(
                        fk_table=fk.table,
                        primary_table=primary_table
                    ))

                sql['FROM'].append('LEFT JOIN {join_module} {join_alias} ON {join_on}.irn={fk_table}.rel_irn'.format(
                    fk_table=fk.table,
                    join_alias=fk.join_alias,
                    join_module=fk.join_module,
                    join_on=fk.join_alias if fk.join_alias else fk.join_module
                ))

        # Special adjustments for the collection specimens dataset
        if self.dataset_name == 'collection-specimens':
            # Add GBIF issues
            sql['FROM'].append('LEFT JOIN gbif ON {primary_table}.guid=gbif.occurrenceid'.format(
                primary_table=primary_table
            ))
            sql['SELECT'].append('gbif.issue as "gbifIssue"')
            sql['SELECT'].append('gbif.id as "gbifID"')

            # Add a few static fields
            sql['SELECT'].append('\'Specimen\' as "basisOfRecord"')
            sql['SELECT'].append('\'NHMUK\' as "institutionCode"')

            # Add _geom field.
            # This is of type LatLonType (https://lucene.apache.org/solr/4_4_0/solr-core/org/apache/solr/schema/LatLonType.html)
            # So lat/lon fields need to be concatenated: lat,lng
            sql['SELECT'].append('CASE WHEN {primary_table}.properties->>\'decimalLatitude\' IS NOT NULL THEN CONCAT_WS(\',\', {primary_table}.properties->>\'decimalLatitude\', {primary_table}.properties->>\'decimalLongitude\') ELSE NULL END as "geom"'.format(
                primary_table=primary_table
            ))

        return sql

    def get_query(self, encode):
        query = OrderedDict(
            [('name', self.dataset_name.rstrip('s'))]
        )
        sql = self.get_sql()
        # Base query uses just the full SQL statement
        query['query'] = self._sql_to_string(sql)
        # Make a copy of where - we're going to needed it in the delta queries
        where = sql['WHERE']

        # For delta query, replace the where statement with delta id
        # The filtering on record type etc., will be handled by the delta queries
        sql['WHERE'] = ["ecatalogue.irn = '${dih.delta._id}'"]
        query['deltaImportQuery'] = self._sql_to_string(sql)
        # Get IRN of records created or modified since last index
        # Uses same filters as default query
        query['deltaQuery'] = """
            SELECT irn AS _id FROM ecatalogue WHERE (modified > '${dih.last_index_time}' OR created > '${dih.last_index_time}') AND (%s)
        """ % ' AND '.join(where)

        # Get IRN of records deleted since last index time
        # Filter on deleted > index time and record type
        query['deletedPkQuery'] = """
            SELECT irn AS _id FROM ecatalogue WHERE deleted > '${dih.last_index_time}' AND ecatalogue.record_type IN ('%s')
        """ % ('\',\''.join(self.dataset.record_types))

        # If encoded, escape quotes so this is ready to be dropped into a solr schema doc
        if encode:
            for k, v in query.items():
                query[k] = self._escape_quotes(v)

        return self._dict2xml(query, 'entity')

    def get_schema(self):
        # Create basic schema fields with _id and multimedia
        schema_fields = [
            OrderedDict(name="_id", type="int", indexed="true", stored="true", required="true"),
            OrderedDict(name=self.multimedia_field, type="string", indexed="false", stored="true", required="false"),
            OrderedDict(name=self.guid_field, type="string", indexed="false", stored="true", required="false"),
            OrderedDict(name="imageCategory", type="semiColonDelimited", indexed="true", stored="true", required="false", multiValued="true"),
            OrderedDict(name="created", type="date", indexed="true", stored="true", required="true"),
            OrderedDict(name="modified", type="date", indexed="true", stored="true", required="false"),
            OrderedDict(name="_has_multimedia", type="boolean", indexed="true", stored="false", required="false", default="false"),
            # sumPreferredCentroidLatDec is populated even when lat/lng is not centroid!
            # What field denotes centroid?
            # OrderedDict(name="centroid", type="boolean", indexed="true", stored="false", required="false", default="false"),
         ]

        # Create a list of schema fields already added
        manual_schema_fields = [f['name'] for f in schema_fields]

        # Add in all dataset defined fields
        for field in self.dataset_fields:
            # Do not add field if we've already added it manually
            if field.field_alias not in manual_schema_fields:
                schema_fields.append(OrderedDict(name=field.field_alias, type="field_text", indexed="true", stored="true", required="false"))

        # Add additional collection specimen fields - GBIF issues and static fields
        if self.dataset_name == 'collection-specimens':
            schema_fields.append(OrderedDict(name="geom", type="geospatial_rpt", indexed="true", stored="false", required="false"))
            schema_fields.append(OrderedDict(name="gbifIssue", type="semiColonDelimited", indexed="true", stored="true", required="false", multiValued="true"))
            schema_fields.append(OrderedDict(name="gbifID", type="field_text", indexed="false", stored="true", required="false"))
            schema_fields.append(OrderedDict(name="basisOfRecord", type="field_text", indexed="false", stored="true", required="false"))
            schema_fields.append(OrderedDict(name="institutionCode", type="field_text", indexed="false", stored="true", required="false"))

        return self._dict2xml(schema_fields, 'field')

    @staticmethod
    def _sql_to_string(sql):
        """
        Convert SQL dict to a string
        @param sql:
        @return:
        """
        sql_string = ''
        for key, value in sql.items():
            sql_string += ' %s ' % key
            if key in ['SELECT', 'GROUP BY']:
                conj = ', '
            elif key == 'WHERE':
                conj = ' AND '
            else:
                conj = ' '
            sql_string += conj.join(value)

        return sql_string

    def _dict2xml(self, d, root_node=None):
        wrap = False if None is root_node or isinstance(d, list) else True
        root = 'objects' if None is root_node else root_node
        root_singular = root[:-1] if 's' == root[-1] and None == root_node else root
        xml = ''
        children = []

        if isinstance(d, dict):
            for key, value in dict.items(d):
                if isinstance(value, dict):
                    children.append(self._dict2xml(value, key))
                elif isinstance(value, list):
                    children.append(self._dict2xml(value, key))
                else:
                    xml = xml + ' ' + key + '="' + str(value) + '"'
        else:
            for value in d:
                children.append(self._dict2xml(value, root_singular))

        end_tag = '>' if 0 < len(children) else '/>'

        if wrap or isinstance(d, dict):
            xml = '<' + root + xml + end_tag

        if 0 < len(children):
            for child in children:
                xml = xml + child

            if wrap or isinstance(d, dict):
                xml = xml + '</' + root + '>'

        return xml

    @staticmethod
    def _escape_quotes(sql):
        """
        If this is to be used in a SOLR Xml schema, need to escape
        Quotes - ' & "
        @param sql:
        @return: escaped str
        """
        return sql.replace('\'', '&#39;').replace('"', '&quot;').replace('<', '&lt;')
