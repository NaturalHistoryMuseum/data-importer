from functools import partial
from typing import Union

from dataimporter.cli.utils import console
from dataimporter.importer import DataImporter


def setup_env(importer: DataImporter) -> dict:
    """
    Returns a dict of variables to made available in the maintenance shell.

    :param importer: a DataImporter instance
    :return: a dict
    """
    return {
        'importer': importer,
        'console': console,
        # some convenience functions for printing data
        'pcd': partial(print_record_data, importer, 'ecatalogue'),
        'ptd': partial(print_record_data, importer, 'etaxonomy'),
        'pmd': partial(print_record_data, importer, 'emultimedia'),
        'pgd': partial(print_record_data, importer, 'gbif'),
        'cm': partial(check_membership, importer),
    }


def print_record_data(importer: DataImporter, name: str, record_id: Union[str, int]):
    """
    A convenience function for printing record data from the given data store.

    :param importer: a DataImporter instance
    :param name: the store to look up the record ID in
    :param record_id: a record ID (can be int or str, we deal with it)
    """
    store = importer.get_store(name)
    if store is None:
        console.print('store not found', style='red')
        return
    record = store.get_record(str(record_id))
    if record is None:
        console.print('record not found', style='red')
        return
    console.print(record.data)


def check_membership(importer: DataImporter, name: str, record_id: Union[str, int]):
    """
    A convenience function which checks if the given record ID is a member of the given
    view. If it isn't, the reason why is printed.

    :param importer: a DataImporter instance
    :param name: name of the view
    :param record_id: record ID (can be int or str, we deal with it)
    """
    view = importer.get_view(name)
    if view is None:
        console.print('view not found', style='red')
        return
    record = view.store.get_record(str(record_id))
    if record is None:
        console.print('record not found', style='red')
        return
    result = view.is_member(record)
    if result:
        print(f'{record_id} is a member of {name}')
    else:
        print(f"{record_id} is not a member of {name} due to '{result.reason}'")
