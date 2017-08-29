#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '22/03/2017'.
"""

import os
import re
import glob

from ke2sql.lib.config import Config


def get_dataset_tasks():
    """
    Get a list of all dataset tasks
    :return:
    """
    from ke2sql.tasks.specimen import SpecimenDatasetTask
    from ke2sql.tasks.indexlot import IndexLotDatasetTask
    from ke2sql.tasks.artefact import ArtefactDatasetTask
    return [IndexLotDatasetTask]
    return [SpecimenDatasetTask, IndexLotDatasetTask, ArtefactDatasetTask]



def list_all_modules():
    """
    Build a list of all unique module names
    :return:
    """
    file_tasks = get_dataset_tasks()
    modules = set()
    [[modules.add(field.module_name) for field in file_task.fields] for file_task in file_tasks]
    return list(modules)


def get_file_export_dates():
    """
    Get a list of all file export dates
    :return:
    """
    full_export_date = Config.getint('keemu', 'full_export_date')
    export_dir = Config.get('keemu', 'export_dir')
    file_pattern = 'ecatalogue.export.*.gz'
    re_date = re.compile(r"([0-9]+)\.gz$")
    export_dates = []
    # Loop through all the files in the export directory,
    # Building a list of dates that need to br processed
    for fn in glob.glob(os.path.join(export_dir, file_pattern)):
        m = re_date.search(fn)
        file_date = int(m.group(1))
        if file_date >= full_export_date:
            export_dates.append(file_date)
    # Sort in reverse date order
    export_dates.sort()
    return export_dates
