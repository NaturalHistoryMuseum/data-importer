#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '20/03/2017'.
"""

def dataset_task_list():
    """
    List all datasets
    :return:
    """
    return [ArtefactDatasetTask, IndexLotDatasetTask, SpecimenDatasetTask]


def dataset_get_module_fields(module):
    """
    Get a list of all the fields in a dataset for a particular module
    :param module: ecatalogue etc.,
    :return:
    """
    fields = []
    for dataset_task in dataset_task_list():
        for field, field_alias in dataset_task.properties:
            field_module, field_name = field.split('.')
            if field_module == module:
                fields.append((field_name, field_alias))

    return fields
