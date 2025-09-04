from functools import partial
from typing import Dict, List, Tuple, Union

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
        'find_unsuitable': partial(find_unsuitable_records, importer),
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
    member_result = view.is_member(record)
    publish_result = view.is_publishable(record)
    if member_result and publish_result:
        console.print(f'{record_id} is a currently published member of {name}')
    elif member_result and not publish_result:
        console.print(
            f'{record_id} is a member of {name}, but is not published due to '
            f'{publish_result.reason}'
        )
    else:
        console.print(
            f"{record_id} is not a member of {name} due to '{member_result.reason}'"
        )


def find_unsuitable_records(
    importer: DataImporter, view_name: str, skip_deleted: bool = True
) -> Dict[str, List[Tuple[str, str]]]:
    """
    Finds and returns lists of records in a view that are no longer members or no longer
    match the publishing rules.Effectively a dry run of purge_unsuitable_records.

    :param importer: a DataImporter instance
    :param view_name: name of the view
    :param skip_deleted: whether to skip deleted records when iterating over mongo
        records (True, default), or return a report on these too (False)
    :return: a dict
    """
    non_publishable_records = []
    non_member_records = []
    view = importer.get_view(view_name)
    db = importer.get_database(view_name)
    total = 0
    deleted = 0
    missing_source = 0
    not_member = 0
    not_publishable = 0
    acceptable = 0
    if skip_deleted:
        filter_kwargs = {'filter': {'data._id': {'$exists': True}}}
    else:
        filter_kwargs = {}
    for mongo_record in db.iter_records(**filter_kwargs):
        total += 1
        if mongo_record.is_deleted:
            deleted += 1
            continue
        source_record = view.store.get_record(mongo_record.id)
        if not source_record:
            missing_source += 1
            continue
        is_member = view.is_member(source_record)
        if not is_member:
            not_member += 1
            non_member_records.append([source_record.id, is_member.reason])
            continue
        is_publishable = view.is_publishable(source_record)
        if not is_publishable:
            not_publishable += 1
            non_publishable_records.append([source_record.id, is_publishable.reason])
            continue
        acceptable += 1
    console.print(
        f'total: {total} | deleted: {deleted} | missing: {missing_source} | not '
        f'members: {not_member} | not publishable: {not_publishable} | acceptable: '
        f'{acceptable}'
    )
    return {
        'not_publishable': non_publishable_records,
        'not_member': non_member_records,
    }
