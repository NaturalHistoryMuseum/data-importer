#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '31/05/2017'.
"""

import click
import logging
from data_importer.commands.solr import SolrCommand

logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
logger = logging.getLogger('luigi-interface')


@click.command()
@click.option('--dataset-name', help='Output a SOLR schema.', required=True)
@click.option('--encode', is_flag=True)
def solr_query(dataset_name, encode=False):
    solr_cmd = SolrCommand(dataset_name)
    sql = solr_cmd.get_sql()
    if encode:
        sql = _escape_quotes(sql)
    print(sql)


def _escape_quotes(sql):
    """
    If this is to be used in a SOLR Xml schema, need to escape
    Quotes - ' & "
    @param sql:
    @return: escaped str
    """
    return sql.replace('\'', '&#39;').replace('"', '&quot;').replace('<', '&lt;')


if __name__ == "__main__":
    solr_query()
