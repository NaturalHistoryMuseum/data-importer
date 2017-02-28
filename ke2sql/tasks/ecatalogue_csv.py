
import luigi

from ke2sql.tasks.ecatalogue import ECatalogueTask
from ke2sql.models.catalogue import CatalogueModel


class ECatalogueCSVTask(ECatalogueTask):
    pass


if __name__ == '__main__':
    luigi.run(main_task_cls=ECatalogueCSVTask)
