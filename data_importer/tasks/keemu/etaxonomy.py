#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '30/08/2017'.
"""

import luigi
from data_importer.lib.filter import Filter
from data_importer.lib.operators import is_uuid
from data_importer.tasks.keemu.base import KeemuBaseTask


class ETaxonomyTask(KeemuBaseTask):
    """
    Task for importing the KE EMu etaxonomy module
    """
    module_name = 'etaxonomy'

    # Ensure etaxonomy tasks runs before FileTask as both are
    # requirements of EcatalogueTask
    priority = 100

    # use all the same filters as the base task but exclude the guid filter
    record_filters = [f for f in KeemuBaseTask.record_filters
                      if f.field_name != 'AdmGUIDPreferredValue']
    # add a guid filter which allows None guids or if a guid exists, forces it
    # to meet the is_uuid standard
    record_filters.append(Filter('AdmGUIDPreferredValue',
                                 [lambda g: g is None or is_uuid(g)]))

    def _record_to_core_dict(self, record):
        """
        Convert record object to the core dict data allowing the GUID to be None
        :param record:
        :return:
        """
        return {
            'irn': record.irn,
            # get the guid or None
            'guid': getattr(record, 'AdmGUIDPreferredValue', None),
            'properties': self._record_map_fields(record, self.record_properties),
            'import_date': int(self.date)
        }


if __name__ == "__main__":
    luigi.run(main_task_cls=ETaxonomyTask)
