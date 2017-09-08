#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '31/08/2017'.
"""


def dataset_get_tasks():
    from data_importer.tasks.indexlot import IndexLotDatasetTask
    from data_importer.tasks.specimen import SpecimenDatasetTask
    from data_importer.tasks.artefact import ArtefactDatasetTask
    return [SpecimenDatasetTask, IndexLotDatasetTask, ArtefactDatasetTask]


def dataset_get_properties(module_name):
    """
    Loop through all of the datasets, and extract all the of property fields
    For a particular module name
    @param module_name:
    @return: list of properties
    """
    record_properties = []
    for dataset_task in dataset_get_tasks():
        record_properties += [f for f in dataset_task.fields if f.module_name == module_name]
    return record_properties


def dataset_get_foreign_keys(module_name=None):
    """
    Build a list of all foreign key fields
    :return:
    """
    foreign_keys = set()
    for dataset_task in dataset_get_tasks():
        for foreign_key in dataset_task.foreign_keys:
            if not module_name:
                foreign_keys.add(foreign_key)
            elif foreign_key.module_name == module_name:
                foreign_keys.add(foreign_key)
    return foreign_keys



