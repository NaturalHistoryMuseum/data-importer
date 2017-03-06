#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '03/03/2017'.
"""

import luigi

from ke2sql.tasks import *

luigi.run(main_task_cls=ECatalogueUpdateTask)
