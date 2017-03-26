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
# from ke2sql.models import EMultimediaModel, ECatalogueModel
from ke2sql.models import ETaxonomyModel

# self.file_copy = (self.date == Config.getint('keemu', 'full_export_date'))


@click.command()
@click.option('--limit', default=None, help='Number of records to process.', type=click.FLOAT)
def run_import(limit):

    date = '20161208'

    cfg = config()

    connection = db.get_connection()
    start_time = time.time()

    for cls in [ETaxonomyModel]:
        model = cls()

        # FIXME: Need to auto-generate this from the fields - move to the model

        file_name = '{model_name}.export.{date}.gz'.format(
            model_name=model.__tablename__,
            date=date
        )
        file_path = os.path.join(cfg.get('keemu', 'export_dir'), file_name)
        counter = {
            'records': 0,
            'inserted': 0
        }
        record_buffer = []
        buffer_length = 10000
        for record in get_records(file_path):
            if model.is_importable(record):
                # Insert multiple records
                # http://docs.sqlalchemy.org/en/latest/core/dml.html#sqlalchemy.sql.expression.Insert.values.params.*args
                record_buffer.append({
                    'irn': record.irn,
                    'properties': Json(model.get_properties(record))
                })
            else:
                # Ensure it doesn't exist? But only for non-full imports.
                pass
            counter['records'] += 1
            if len(record_buffer) == buffer_length:
                connection.execute(sql, record_buffer)
                counter['inserted'] += buffer_length
                print('Writing buffer - {inserted} ({records})'.format(**counter))
                record_buffer = []

            # If we've specified a limit, then end process once we've reached it
            if limit and counter['records'] >= limit:
                counter['inserted'] += len(record_buffer)
                connection.execute(model.sql, record_buffer)
                break

        print('Inserted {0} {1} records in {2} seconds'.format(counter['inserted'], model.__tablename__, time.time() - start_time))

# import io
# import zlib
# from file_read_backwards import FileReadBackwards
#
# def run_import():
#
#     start_time = time.time()
#     model = 'ecatalogue'
#     date = '20161208'
#     cfg = config()
#
#     file_name = '{model_name}.export.{date}.gz'.format(
#                 model_name=model,
#                 date=date
#             )
#
#     file_path = os.path.join(cfg.get('keemu', 'export_dir'), file_name)
#
#     # f = FileReadBackwards(file_path)
#     # for l in f:
#     #     print(l)
#
#     f = gzip.open(file_path, 'rb')
#
#     f.seek(0, 2)
#
#
#     #
#     # print(f.read(-10))
#
#     # for l in f:
#     #     print(l)
#
#     print('{0} seconds'.format(time.time() - start_time))
#
#     # print(f.next())
#
#     # print(file_path)
#     #
#     # with open(file_path, 'rb') as inf:
#     #     top = inf.readlines()[0:500]
#     #
#     # with open(file_path) as inf:
#     #     inf.buffer.seek(-1000, 2)
#     #     inf = io.TextIOWrapper(inf.buffer, encoding=inf.encoding, errors='ignore')
#     #     bottom = inf.readlines()
#     #
#     # x = ''.join(top) + ''.join(bottom)
#     #
#     # # f = ''.join(top + bottom)
#     # print(x)
#
#
#         # inf.buffer.seek(-500, 2)
#         # inf = io.TextIOWrapper(inf.buffer, encoding=inf.encoding, errors='ignore')
#         # x = ''.join(inf.readlines())
#         # decompressed_data = zlib.decompress(x, 16 + zlib.MAX_WBITS)
#         # print(decompressed_data)
#
#     # for line in tailf(file_path):
#     #     print(line)

if __name__ == "__main__":
    run_import()