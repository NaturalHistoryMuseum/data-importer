
import luigi

from ke2sql.tasks.base import BaseTask
from ke2sql.models.catalogue import CatalogueModel


class ECatalogueTask(BaseTask):

    model = CatalogueModel

    record_filters = {
        # List of record types (ColRecordType) to exclude from the import
        'excluded_types': [
            'Acquisition',
            'Bound Volume',
            'Bound Volume Page',
            'Collection Level Description',
            'DNA Card',  # 1 record, but keep an eye on this
            'Field Notebook',
            'Field Notebook (Double Page)',
            'Image',
            'Image (electronic)',
            'Image (non-digital)',
            'Image (digital)',
            'Incoming Loan',
            'L&A Catalogue',
            'Missing',
            'Object Entry',
            'object entry',  # FFS
            'Object entry',  # FFFS
            'PEG Specimen',
            'PEG Catalogue',
            'Preparation',
            'Rack File',
            'Tissue',  # Only 2 records. Watch.
            'Transient Lot'
        ],
        'excluded_statuses': [
            "DELETE",
            "DELETE-MERGED",
            "DUPLICATION",
            "Disposed of",
            "FROZEN ARK",
            "INVALID",
            "POSSIBLE TYPE",
            "PROBLEM",
            "Re-registered in error",
            "Reserved",
            "Retired",
            "Retired (see Notes)",
            "Retired (see Notes)Retired (see Notes)",
            "SCAN_cat",
            "See Notes",
            "Specimen missing - see notes",
            "Stub",
            "Stub Record",
            "Stub record"
        ],
        'collection_departments': [
            "Botany",
            "Entomology",
            "Mineralogy",
            "Palaeontology",
            "Zoology"
        ]
    }

    def _is_importable(self, record):
        """
        Override base is_importable
        Add additional checks for:
            GUID - Must have GUID
            Record status - not on of the excluded record status
            Record type - not one of the excluded record types
            Collection department - one of the known departments
            record type
        :param record:
        :return: boolean - false if not importable
        """

        # Records must have a GUID
        if not getattr(record, 'AdmGUIDPreferredValue', None):
            return False

        # Does this record have an excluded status - Stub etc.,
        record_status = getattr(record, 'SecRecordStatus', None)
        if record_status in self.record_filters['excluded_statuses']:
            return False

        # Does this record have an excluded status - Stub etc.,
        record_type = getattr(record, 'ColRecordType', None)
        if record_type in self.record_filters['excluded_types']:
            return False

        # Record must be in one of the known collection departments
        # (Otherwise the home page breaks)
        collection_department = getattr(record, 'ColDepartment', None)
        if collection_department not in self.record_filters['collection_departments']:
            return False

        # Run the record passed the base filter (checks AdmPublishWebNoPasswordFlag)
        return super(ECatalogueTask, self)._is_importable(record)    


if __name__ == '__main__':
    luigi.run(main_task_cls=ECatalogueTask)
