from dataimporter.emu.views.utils import (
    NO_PUBLISH,
    is_web_published,
    is_valid_guid,
    INVALID_GUID,
)
from dataimporter.emu.views.utils import emu_date
from dataimporter.model import SourceRecord
from dataimporter.view import FilterResult, View, SUCCESS_RESULT

MULTIMEDIA_NOT_URL = FilterResult(False, "Multimedia not a URL")
INVALID_PUBLISHER = FilterResult(False, "Invalid 3D publisher")
NOT_SPECIMEN = FilterResult(False, "3D scan is not a specimen")

VALID_PUBLISHERS = {"sketchfab", "morphosource"}


# 3709063 is a sketchfab one
# emultimedia.export.20230316.gz is a morphosource one


class ThreeDView(View):
    """
    View for 3D records.

    This view doesn't have a Data Portal resource that it populates, instead the records
    that go through this view are embedded within other record types.
    """

    def is_member(self, record: SourceRecord) -> FilterResult:
        """
        Filters the given record, determining whether it is a 3D or not.

        :param record: the record to filter
        :return: a FilterResult object
        """
        if record.get_first_value("MulDocumentType") != "U":
            return MULTIMEDIA_NOT_URL

        if not is_valid_guid(record):
            return INVALID_GUID

        if not is_web_published(record):
            return NO_PUBLISH

        if (
            record.get_first_value("DetPublisher", default="").lower()
            not in VALID_PUBLISHERS
        ):
            return INVALID_PUBLISHER

        # TODO: do we really need this?
        if record.get_first_value("DetResourceType") != "Specimen":
            return NOT_SPECIMEN

        return SUCCESS_RESULT

    def make_data(self, record: SourceRecord) -> dict:
        """
        Converts the record's raw data to a dict which will then be embedded in other
        records and presented on the Data Portal.

        :param record: the record to project
        :return: a dict containing the data for this record that should be displayed on
                 the Data Portal
        """
        # cache for perf
        get_first = record.get_first_value

        asset_id = get_first("AdmGUIDPreferredValue")
        return {
            "_id": record.id,
            "created": emu_date(
                get_first("AdmDateInserted"), get_first("AdmTimeInserted")
            ),
            "modified": emu_date(
                get_first("AdmDateModified"), get_first("AdmTimeModified")
            ),
            "assetID": asset_id,
            "identifier": get_first("MulIdentifier"),
            "title": get_first("MulTitle"),
            "creator": get_first("MulCreator"),
            "category": get_first("DetResourceType"),
            # TODO: what should this be? InteractiveResource?
            "type": "3D",
            "license": "http://creativecommons.org/licenses/by/4.0/",
            # TODO: RightsSummaryDataLocal?
            "rightsHolder": "The Trustees of the Natural History Museum, London",
        }
