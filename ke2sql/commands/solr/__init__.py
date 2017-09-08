#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '04/09/2017'.
"""

import click
from ke2sql.lib.dataset import dataset_get_tasks
from collections import OrderedDict


class SolrCommand(object):
    def __init__(self, dataset_name):
        self.dataset = self._get_dataset(dataset_name)

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
        for field in self.dataset.fields:
            if field.module_name is 'emultimedia' or field.field_alias in ['created', 'modified']:
                continue
            yield field

    # Commands
    def get_sql(self):
        primary_table = 'ecatalogue'

        sql = OrderedDict([
            ('SELECT', [
                '{}.irn as _id'.format(primary_table),
                '{}.properties->>\'created\' || \'T00:00:00Z\' as created'.format(primary_table),
                '{}.properties->>\'modified\' || \'T00:00:00Z\' as modified'.format(primary_table),
                """COALESCE(jsonb_agg(jsonb_build_object(
                    'identifier', format('http://www.nhm.ac.uk/services/media-store/asset/%s/contents/preview', emultimedia.properties->>'assetID'),
                    'type', 'StillImage',
                    'license',  'http://creativecommons.org/licenses/by/4.0/',
                    'rightsHolder',  'The Trustees of the Natural History Museum, London'
                    ) || emultimedia.properties) FILTER (WHERE emultimedia.irn IS NOT NULL), NULL)::TEXT as {multimedia_field}
                """.format(multimedia_field=self.dataset.multimedia_field),
                'string_agg(DISTINCT emultimedia.properties->>\'category\', \';\') as imageCategory',
                # We don't index the multimedia data, so we need a flag to denote whether
                # this record has images
                'count(emultimedia.irn)>1 as _has_multimedia'
            ]),
            ('FROM', [primary_table]),
            ('WHERE', [
                'record_type=\'Index Lot\'',
                '({0}.embargo_date IS NULL OR {0}.embargo_date < NOW())'.format(primary_table),
                '{}.deleted IS NULL'.format(primary_table)
            ]),
            ('GROUP BY', ['ecatalogue.irn', 'etaxonomy.irn'])
        ])

        # Allow quick look up of which module fields have been included
        module_names = set()

        for field in self.dataset_fields:
            sql['SELECT'].append('{0}.properties->>\'{1}\' as "{1}"'.format(
                field.module_name,
                field.field_alias
            ))
            module_names.add(field.module_name)

        for fk in self.dataset.foreign_keys:
            # Skip emultimedia
            sql['FROM'].append('LEFT JOIN {fk_table} ON {fk_table}.irn={primary_table}.irn'.format(
                fk_table=fk.table,
                primary_table=primary_table
            ))

            sql['FROM'].append('LEFT JOIN {join_module} ON {join_module}.irn={fk_table}.rel_irn'.format(
                fk_table=fk.table,
                join_module=fk.join_module
            ))

        return self._sql_to_string(sql)

    def get_schema(self):
        # Create basic schema fields with _id and multimedia
        schema_fields = [
            OrderedDict(name="_id", type="int", indexed="true", stored="true", required="true"),
            OrderedDict(name=self.dataset.multimedia_field, type="string", indexed="false", stored="true", required="false"),
            OrderedDict(name="imageCategory", type="semiColonDelimited", indexed="true", stored="true", required="false", multiValued="true"),
            OrderedDict(name="created", type="date", indexed="true", stored="true", required="true"),
            OrderedDict(name="modified", type="date", indexed="true", stored="true", required="false"),
            OrderedDict(name="_has_multimedia", type="boolean", indexed="true", stored="false", required="false", default="false"),
        ]

        # Add in all dataset defined fields
        for field in self.dataset_fields:
            schema_fields.append(OrderedDict(name=field.field_alias, type="field_text", indexed="true", stored="true", required="false"))

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
