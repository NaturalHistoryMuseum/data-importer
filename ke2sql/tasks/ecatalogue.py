from operator import is_not

from ke2sql.tasks.base import BaseTask
from ke2sql.lib.operators import is_one_of, is_not_one_of


class ECatalogueTask(BaseTask):

    # Add extra fields for multimedia, taxonomy and parent references
    columns = BaseTask.columns + [
        ("record_type", "TEXT"),
        ("multimedia_irns", "INTEGER[]"),  # Currently can't be used as foreign key - but under development
        # Tried foreign key - but KE EMu doesn't enforce referential integrity, so getting foreign key violation
        ("indexlot_taxonomy_irn", "INTEGER"),
        ("parent_irn", "INTEGER"),
        ("parasite_taxonomy_irn", "INTEGER"),
        # Records with embargo date need to be imported, but excluded from dataset
        # If they are filtered out, they will only make it to the portal when updated
        ("embargo_date", "DATE"),
    ]

    filters = {
        # Records must have a GUID
        'AdmGUIDPreferredValue': [
            (is_not, None)
        ],
        # Does this record have an excluded status - Stub etc.,
        'SecRecordStatus': [
            (is_not, None),
            (is_not_one_of, [
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
            ])
        ],
        # Exclude records without the proper record type
        'ColRecordType': [
            (is_not, None),
            (is_not_one_of, [
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
            ])
        ],
        # Record must be in one of the known collection departments
        # (Otherwise the home page breaks)
        'ColDepartment': [
            (is_one_of, [
                "Botany",
                "Entomology",
                "Mineralogy",
                "Palaeontology",
                "Zoology"
            ])
        ],
    }
