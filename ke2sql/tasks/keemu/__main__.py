#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '03/03/2017'.

main_task_cls=ETaxonomyCopyTask

python tasks/ ECatalogueCopyTask --date 20170309 --local-scheduler --limit 1000

python tasks/ ArtefactDatasetTask --date 20170309 --local-scheduler

"""

import luigi

from ke2sql.tasks.keemu import *

if __name__ == "__main__":
    luigi.run(main_task_cls=KeemuCopyTask)
