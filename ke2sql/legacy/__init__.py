#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

from ke2sql.tasks.postgres import PostgresUpsertMixin, PostgresCopyMixin
from ke2sql.tasks.keemu.emultimedia import KeemuEMultimediaMixin
from ke2sql.tasks.keemu.ecatalogue import KeemuECatalogueMixin
from ke2sql.tasks.keemu.etaxonomy import KeemuETaxonomyMixin
from ke2sql.tasks.dataset import (
    ArtefactDatasetTask,
    IndexLotDatasetTask,
    SpecimenDatasetTask
)

__all__ = [
    'EMultimediaUpsertTask',
    'EMultimediaCopyTask',
    'ECatalogueUpsertTask',
    'ECatalogueCopyTask',
    'ETaxonomyUpsertTask',
    'ETaxonomyCopyTask',
    'ArtefactDatasetTask',
    'IndexLotDatasetTask',
    'SpecimenDatasetTask'
]
# Emultimedia tasks

class EMultimediaUpsertTask(KeemuEMultimediaMixin, PostgresUpsertMixin):
    pass


class EMultimediaCopyTask(KeemuEMultimediaMixin, PostgresCopyMixin):
    pass


# ECatalogue tasks

class ECatalogueUpsertTask(KeemuECatalogueMixin, PostgresUpsertMixin):
    pass


class ECatalogueCopyTask(KeemuECatalogueMixin, PostgresCopyMixin):
    pass


# ETaxonomy tasks

class ETaxonomyUpsertTask(KeemuETaxonomyMixin, PostgresUpsertMixin):
    pass


class ETaxonomyCopyTask(KeemuETaxonomyMixin, PostgresCopyMixin):
    pass