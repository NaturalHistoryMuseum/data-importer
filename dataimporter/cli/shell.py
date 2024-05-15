from typing import Union

from functools import partial

from dataimporter.cli.utils import console
from dataimporter.importer import DataImporter


def setup_env(importer: DataImporter) -> dict:
    """
    Returns a dict of variables to made available in the maintenance shell.

    :param importer: a DataImporter instance
    :return: a dict
    """
    return {
        "importer": importer,
        "console": console,
        # some convenience functions for printing data
        "prd": partial(print_record_data, importer),
        "pcd": partial(print_record_data, importer, "ecatalogue"),
        "ptd": partial(print_record_data, importer, "etaxonomy"),
        "pmd": partial(print_record_data, importer, "emultimedia"),
    }


def print_record_data(importer: DataImporter, table: str, record_id: Union[str, int]):
    """
    A convenience function for printing record data from the given EMu table.

    :param importer: a DataImporter instance
    :param table: the EMu table to look up the record ID in
    :param record_id: an EMu record ID (can be int or str, we deal with it)
    """
    data_db = importer.dbs.get(table)
    if data_db is None:
        console.print("table not found", style="red")
        return
    record = data_db.get_record(str(record_id))
    if record is None:
        console.print("record not found", style="red")
    console.print(record.data)
