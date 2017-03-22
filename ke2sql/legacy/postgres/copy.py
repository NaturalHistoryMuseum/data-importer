#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '03/03/2017'.
"""

import sys
import json
from collections import OrderedDict
from luigi.contrib.postgres import CopyToTable as LuigiCopyToTable
from prompter import prompt, yesno


class PostgresCopyMixin(LuigiCopyToTable):
    """
    Extending CopyToTable with row mapper
    """
    def rows(self):
        """
        Implementation of CopyToTable.rows()
        :return:
        """
        # Populate row using the same order as columns
        ordered_cols = OrderedDict(self.columns).keys()
        # Format a field value - e.g. if dict -> json.dumps
        formatters = {
            dict: json.dumps,
            list: self.pg_array_str
        }
        # Loop through records, building and yielding rows (lists)
        for record in self.records():
            row = []
            record['created'] = 'NOW()'
            for col in ordered_cols:
                value = record.get(col, None)
                # If we have a value, run the value through the formatters (list, dict)
                if value:
                    for formatter_type, formatter in formatters.items():
                        if type(value) is formatter_type:
                            value = formatter(value)
                row.append(value)
            yield row

    def delete_record(self, record):
        # No need to delete on copy
        pass

    @staticmethod
    def pg_array_str(value):
        """
        Convert a list to a postgres array string, suitable for using in copy
        :param value:
        :return: str {1,2,3}
        """
        return '{' + ','.join(value) + '}'

    def run(self):
        connection = self.output().connect()
        if self.table_exists(connection):
            if yesno('Your are performing a full import - all existing {table} data will be deleted. Are you sure you want to continue?'.format(
                    table=self.table
            )):
                self.drop_table(connection)
            else:
                sys.exit("Import cancelled")

        super(PostgresCopyMixin, self).run()

    def post_copy(self, connection):
        """
        After copying, create the indexes - this speeds up data ingestion
        :param connection:
        :return:
        """
        self.ensure_indexes(connection)
