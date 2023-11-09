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


class ArtefactView(View):
    """
    View for artefact records.

    This view populates the artefacts resource on the Data Portal.
    """

    def is_member(self, record: SourceRecord) -> FilterResult:
        """
        Filters the given record, determining whether it should be included in the
        artefact resource or not.

        :param record: the record to filter
        :return: a FilterResult object
        """
        if record.get_first_value("ColRecordType") != "Artefact":
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
        # cache this for perf
        get_first = record.get_first_value

        return {
            "_id": record.id,
            "created": emu_date(
                get_first("AdmDateInserted"), get_first("AdmTimeInserted")
            ),
            "modified": emu_date(
                get_first("AdmDateModified"), get_first("AdmTimeModified")
            ),
            "artefactName": get_first("PalArtObjectName"),
            # seems to not be populated in any of the current artefact records
            "artefactType": get_first("PalArtType"),
            "artefactDescription": get_first("PalArtDescription"),
            "scientificName": get_first("IdeCurrentScientificName"),
        }
