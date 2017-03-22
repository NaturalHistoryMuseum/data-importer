#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '20/03/2017'.
"""

import luigi
from ke2sql.tasks.postgres import PostgresUpsertMixin, PostgresCopyMixin
from ke2sql.tasks.keemu.mixin import KeemuMixin


class KeemuUpsertTask(KeemuMixin, PostgresUpsertMixin):
    pass


class KeemuCopyTask(KeemuMixin, PostgresCopyMixin):
    pass

__all__ = [
    'KeemuUpsertTask',
    'KeemuCopyTask'
]