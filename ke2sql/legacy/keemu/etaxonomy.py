
from ke2sql.tasks.keemu.base import KeemuBaseMixin
from ke2sql.tasks.dataset import dataset_get_module_fields


class KeemuETaxonomyMixin(KeemuBaseMixin):

    module_name = 'etaxonomy'

    # Automatically build field mappings form the dataset fields
    fields = dataset_get_module_fields(module_name)
