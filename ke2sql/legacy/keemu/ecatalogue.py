from operator import is_not

from ke2sql.tasks.keemu.base import KeemuBaseMixin
from ke2sql.lib.operators import is_one_of, is_not_one_of
from ke2sql.tasks.dataset import dataset_get_module_fields, dataset_get_all_record_types


class KeemuECatalogueMixin(KeemuBaseMixin):

    # module_name = 'ecatalogue'
    #
    # fields = dataset_get_module_fields(module_name)

    # Add extra fields for multimedia, taxonomy and parent references
    columns = KeemuBaseMixin.columns + [
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

    # filters = {
    #     # Records must have a GUID
    #     'AdmGUIDPreferredValue': [
    #         (is_not, None)
    #     ],
    #     # Does this record have an excluded status - Stub etc.,
    #     'SecRecordStatus': [
    #         (is_not, None),
    #         (is_not_one_of, [
    #             "DELETE",
    #             "DELETE-MERGED",
    #             "DUPLICATION",
    #             "Disposed of",
    #             "FROZEN ARK",
    #             "INVALID",
    #             "POSSIBLE TYPE",
    #             "PROBLEM",
    #             "Re-registered in error",
    #             "Reserved",
    #             "Retired",
    #             "Retired (see Notes)",
    #             "Retired (see Notes)Retired (see Notes)",
    #             "SCAN_cat",
    #             "See Notes",
    #             "Specimen missing - see notes",
    #             "Stub",
    #             "Stub Record",
    #             "Stub record"
    #         ])
    #     ],
    #     # Exclude records without the proper record type
    #     'ColRecordType': [
    #         (is_not, None),
    #         # Record type must be one defined in a dataset task
    #         (is_one_of, dataset_get_all_record_types())
    #     ],
    #     # Record must be in one of the known collection departments
    #     # (Otherwise the home page breaks)
    #     'ColDepartment': [
    #         (is_one_of, [
    #             "Botany",
    #             "Entomology",
    #             "Mineralogy",
    #             "Palaeontology",
    #             "Zoology"
    #         ])
    #     ],
    # }
