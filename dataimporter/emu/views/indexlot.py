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
from dataimporter.model import SourceRecord
from dataimporter.view import View, FilterResult, SUCCESS_RESULT


class IndexLotView(View):
    """
    View for index lot records.

    This view populates the index lot resource on the Data Portal.
    """

    def is_member(self, record: SourceRecord) -> FilterResult:
        """
        Filters the given record, determining whether it should be included in the index
        lot resource or not.

        :param record: the record to filter
        :return: a FilterResult object
        """
        if record.get_first_value("ColRecordType") != "Index Lot":
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
            "material": get_first("EntIndMaterial"),
            "type": get_first("EntIndType"),
            "media": get_first("EntIndMedia"),
            "british": get_first("EntIndBritish"),
            "kindOfMaterial": get_first("EntIndKindOfMaterial"),
            "kindOfMedia": get_first("EntIndKindOfMedia"),
            "materialCount": get_all("EntIndCount"),
            "materialSex": get_all("EntIndSex"),
            "materialStage": get_all("EntIndStage"),
            "materialTypes": get_all("EntIndTypes"),
            "materialPrimaryTypeNumber": get_all("EntIndPrimaryTypeNo"),
        }
