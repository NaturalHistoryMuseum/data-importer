#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '20/03/2017'.
"""

from .artefact import ArtefactDatasetTask
from .indexlot import IndexLotDatasetTask
from .specimen import SpecimenDatasetTask


def dataset_tasks():
    """
    Return a list of all dataset tasks
    :return:
    """
    return [ArtefactDatasetTask, IndexLotDatasetTask, SpecimenDatasetTask]


def dataset_get_module_fields(module_name):
    """
    Get a list of all the fields in a dataset for a particular module
    :param module_name: ecatalogue etc.,
    :return:
    """
    fields = []
    for dataset_task in dataset_tasks():
        for field, field_alias in dataset_task.fields:
            field_module, field_name = field.split('.')
            if field_module == module_name:
                fields.append((field_name, field_alias))

    return fields


def dataset_get_all_record_types():
    """
    Get a list of the record types across all datasets
    :return: list
    """
    record_types = set()
    for dataset_task in dataset_tasks():
        record_types |= set(dataset_task.record_types)
    return list(record_types)
