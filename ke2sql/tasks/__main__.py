#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '03/03/2017'.

main_task_cls=ETaxonomyCopyTask

python tasks/ ECatalogueCopyTask --date 20170309 --local-scheduler --limit 1000

python tasks/ ArtefactDatasetTask --date 20170309 --local-scheduler

"""

import luigi

from ke2sql.tasks.delete import DeleteTask
from ke2sql.tasks.specimen import SpecimenDatasetTask
from ke2sql.tasks.indexlot import IndexLotDatasetTask
from ke2sql.tasks.artefact import ArtefactDatasetTask
from ke2sql.lib.config import Config
from ke2sql.lib.helpers import get_dataset_tasks

class MainTask(luigi.Task):
    """
    Convenience function for running all dataset and delete tasks
    """
    date = luigi.IntParameter()
    limit = luigi.IntParameter(default=None, significant=False)

    def requires(self):
        # Do not run the delete task, if this is the full export date
        full_export_date = Config.getint('keemu', 'full_export_date')
        if full_export_date != self.date:
            yield DeleteTask(date=self.date)
        #  Get all params and run all of the dataset tasks
        params_dict = self.get_params_dict()
        for task in get_dataset_tasks():
            yield task(**params_dict)

    def get_params_dict(self):
        """
        Build the param and values dict for the task
        :return:
        """
        params_dict = {}
        for param_name, param_type in self.get_params():
            params_dict[param_name] = getattr(self, param_name)
        return params_dict


if __name__ == "__main__":
    luigi.run(main_task_cls=MainTask)
