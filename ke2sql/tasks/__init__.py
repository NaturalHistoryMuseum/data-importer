#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

from ke2sql.tasks.postgres import UpdateTable, CopyToTable

from .emultimedia import EMultimediaTask
from .ecatalogue import ECatalogueTask

# Emultimedia tasks


class EMultimediaUpdateTask(EMultimediaTask, UpdateTable):
    pass


class EMultimediaCopyTask(EMultimediaTask, CopyToTable):
    pass


# ECatalogue tasks

class ECatalogueUpdateTask(ECatalogueTask, UpdateTable):
    pass


class ECatalogueCopyTask(ECatalogueTask, CopyToTable):
    pass