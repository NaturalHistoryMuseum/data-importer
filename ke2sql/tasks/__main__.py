#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '03/03/2017'.

main_task_cls=ETaxonomyCopyTask

python tasks/ ECatalogueCopyTask --date 20170309 --local-scheduler --limit 10000

"""

import luigi

from ke2sql.tasks import *

luigi.run()
