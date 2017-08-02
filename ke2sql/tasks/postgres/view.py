#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '04/04/2017'.
"""

import abc
import luigi
from luigi.contrib.postgres import PostgresQuery
import logging

from ke2sql.lib.db import db_view_exists, db_drop_view

logger = logging.getLogger('luigi-interface')


class MaterialisedViewTask(PostgresQuery):
    """
    Extends Luigi Postgres Query Task for handling materialised views
    """
    refresh = luigi.BoolParameter(default=True, significant=False)

    @abc.abstractproperty
    def views(self):
        """
        List of materialised views, as tuples
            (view_name: sql)
        :return: list
        """
        return []

    @property
    def query(self):
        """
        Query for building materialised view
        :return:
        """
        connection = self.output().connect()
        # Query list -joined at end so we can have multiple queries
        query = []

        try:
            for view_name, view_sql in self.views:
                # If the view does not exist, create it
                if not db_view_exists(view_name, connection):
                    logger.info('Creating materialized view %s', view_name)
                    query.append('CREATE MATERIALIZED VIEW "{view_name}" AS ({view_sql})'.format(
                        view_name=view_name,
                        view_sql=view_sql
                    ))
                    # Add index on _id column
                    query.append('CREATE UNIQUE INDEX ON "{view_name}"(_id)'.format(
                        view_name=view_name,
                    ))
                # Otherwise, refresh the view (unless no refresh is set)
                elif self.refresh:
                    logger.info('Refreshing materialized view %s', view_name)
                    query.append('REFRESH MATERIALIZED VIEW "{view_name}"'.format(
                        view_name=view_name
                    ))

            # If we have queries, join them into one
            # If no query, use
            query = ';'.join(query) if query else 'SELECT 1'
            logger.info('Running query: %s', query)

        finally:
            connection.close()

        return query
