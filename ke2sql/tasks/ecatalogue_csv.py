
import luigi

from ke2sql.tasks.ecatalogue import ECatalogueTask
from ke2sql.models.catalogue import CatalogueModel


class ECatalogueCSVTask(ECatalogueTask):

    module = 'ecatalogue'

    def write_buffer(self):
        print('WRITE')

    def output(self):
        """
        Returns the target output for this task.
        In this case, a successful execution of this task will create a file on the local filesystem.
        :return: the target output for this task.
        :rtype: object (:py:class:`luigi.target.Target`)
        """
        return luigi.LocalTarget("data/ecatalogue_%s.csv" % self.date)

if __name__ == '__main__':
    luigi.run(main_task_cls=ECatalogueCSVTask)
