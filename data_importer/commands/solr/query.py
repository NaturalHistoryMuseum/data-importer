#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '31/05/2017'.
"""

import click
import logging
from collections import OrderedDict
from data_importer.commands.solr import SolrCommand

logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
logger = logging.getLogger('luigi-interface')


@click.command()
@click.option('--dataset-name', help='Output a SOLR schema.', required=True)
@click.option('--encode', is_flag=True)
def solr_query(dataset_name, encode=False):
    solr_cmd = SolrCommand(dataset_name)
    query = solr_cmd.get_query(encode)
    print(query)


if __name__ == "__main__":
    solr_query()
