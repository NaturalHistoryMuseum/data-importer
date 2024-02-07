from dataimporter.emu.views.utils import (
    NO_PUBLISH,
    DISALLOWED_STATUSES,
    DEPARTMENT_COLLECTION_CODES,
    INVALID_STATUS,
    INVALID_DEPARTMENT,
    INVALID_TYPE,
    is_web_published,
    is_valid_guid,
    INVALID_GUID,
)
from dataimporter.emu.views.utils import emu_date
from dataimporter.lib.model import SourceRecord
from dataimporter.lib.view import View, FilterResult, SUCCESS_RESULT

INVALID_SUB_DEPARTMENT = FilterResult(False, "Invalid sub-department")
INVALID_PROJECT = FilterResult(False, "Invalid project")


class PreparationView(View):
    """
    View for preparation records.

    This view populates the preparation resource on the Data Portal.
    """

    def is_member(self, record: SourceRecord) -> FilterResult:
        """
        Filters the given record, determining whether it should be included in the
        preparation resource or not. This view member filter checks for one of two kinds
        of record in order for the record to be included:

            - a preparation record from the Molecular Collections sub-department
            - a mammal group part record from the LS Mammals sub-department with the
              DToL project tag

        These two types have slightly different fields but are both preps.

        :param record: the record to filter
        :return: a FilterResult object
        """
        record_type = record.get_first_value("ColRecordType", default="").lower()
        sub_department = record.get_first_value("ColSubDepartment", default="").lower()

        if record_type == "preparation":
            # if the record is a prep, it must be a molecular collections prep
            if sub_department != "molecular collections":
                return INVALID_SUB_DEPARTMENT
        elif record_type == "mammal group part":
            # if the record is a mammal group part, it must be a mammals record and be
            # a DToL project record
            if sub_department != "ls mammals":
                return INVALID_SUB_DEPARTMENT
            if record.get_first_value("NhmSecProjectName") != "Darwin Tree of Life":
                return INVALID_PROJECT
        else:
            # any other type is invalid
            return INVALID_TYPE

        if not is_web_published(record):
            return NO_PUBLISH

        if not is_valid_guid(record):
            return INVALID_GUID

        if record.get_first_value("SecRecordStatus") in DISALLOWED_STATUSES:
            return INVALID_STATUS

        if record.get_first_value("ColDepartment") not in DEPARTMENT_COLLECTION_CODES:
            return INVALID_DEPARTMENT

        return SUCCESS_RESULT

    def make_data(self, record: SourceRecord) -> dict:
        """
        Converts the record's raw data to a dict which will be the data presented on the
        Data Portal.

        :param record: the record to project
        :return: a dict containing the data for this record that should be displayed on
                 the Data Portal
        """
        # cache these for perf
        get_all = record.get_all_values
        get_first = record.get_first_value

        return {
            "_id": record.id,
            "created": emu_date(
                get_first("AdmDateInserted"), get_first("AdmTimeInserted")
            ),
            "modified": emu_date(
                get_first("AdmDateModified"), get_first("AdmTimeModified")
            ),
            "project": get_all("NhmSecProjectName"),
            "preparationNumber": get_first("EntPreNumber"),
            "preparationType": get_first("EntPrePreparationKind"),
            "mediumType": get_first("EntPreStorageMedium"),
            "preparationProcess": get_first("EntPrePreparationMethod"),
            "preparationContents": get_first("EntPreContents"),
            "preparationDate": get_first("EntPreDate"),
        }
