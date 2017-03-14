#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '14/03/2017'.
"""

import luigi

from ke2sql.tasks import EMultimediaUpdateTask, ECatalogueUpdateTask, ETaxonomyUpdateTask


class DatasetTask(luigi.Task):
    """
    Base CKAN API Task
    """

    # Date to process
    date = luigi.IntParameter()

    def requires(self):
        # Make sure all the postgres update tasks have run
        return [
            EMultimediaUpdateTask(date=self.date),
            ECatalogueUpdateTask(date=self.date),
            EMultimediaUpdateTask(date=self.date)
        ]

if __name__ == "__main__":
    luigi.run(main_task_cls=DatasetTask)
