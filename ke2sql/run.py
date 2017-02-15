#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import os
import gzip
import time
import click
from psycopg2.extras import Json


from ke2sql.lib import config, get_records
from ke2sql import db
from ke2sql.models import EMultimediaModel, ECatalogueModel


@click.command()
@click.option('--limit', default=None, help='Number of records to process.', type=click.FLOAT)
def run_import(limit):

    date = '20160303'

    cfg = config()

    connection = db.get_connection()
    start_time = time.time()

    for cls in [EMultimediaModel, ECatalogueModel]:
        model = cls()
        # For speed, we don't use the sqlalchemy orm for inserts
        # FIXME: Need to auto-generate this from the fields - move to the model
        sql = """
            INSERT INTO {0} (irn, properties, created) VALUES (%(irn)s, %(properties)s, NOW())
            ON CONFLICT (irn)
            DO UPDATE SET (properties, modified) = (%(properties)s, NOW()) WHERE {0}.irn = %(irn)s
        """.format(model.__tablename__)

        file_name = '{model_name}.export.{date}.gz'.format(
            model_name=model.__tablename__,
            date=date
        )
        file_path = os.path.join(cfg.get('keemu', 'export_dir'), file_name)
        counter = {
            'records': 0,
            'inserted': 0
        }
        buffer = []
        buffer_length = 10000
        for record in get_records(file_path):
            if model.is_importable(record):
                # Insert multiple records
                # http://docs.sqlalchemy.org/en/latest/core/dml.html#sqlalchemy.sql.expression.Insert.values.params.*args
                buffer.append({
                    'irn': record.irn,
                    'properties': Json(model.get_properties(record))
                })
            else:
                # Ensure it doesn't exist? But only for non-full imports.
                pass
            counter['records'] += 1
            if len(buffer) == buffer_length:
                connection.execute(sql, buffer)
                counter['inserted'] += buffer_length
                print('Writing buffer - {inserted} ({records})'.format(**counter))
                buffer = []

            # If we've specified a limit, then end process once we've reached it
            if limit and counter['records'] >= limit:
                counter['inserted'] += len(buffer)
                connection.execute(sql, buffer)
                break

        print('Inserted {0} {1} records in {2} seconds'.format(counter['inserted'], model.__tablename__, time.time() - start_time))

if __name__ == "__main__":
    run_import()