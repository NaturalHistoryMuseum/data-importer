from dataimporter.emu.views.utils import NO_PUBLISH, is_web_published
from dataimporter.emu.views.utils import emu_date
from dataimporter.model import SourceRecord
from dataimporter.view import FilterResult, View, SUCCESS_RESULT

MULTIMEDIA_NOT_IMAGE = FilterResult(False, "Multimedia not an image")


class ImageView(View):
    """
    View for image records.

    This view doesn't have a Data Portal resource that it populates, instead the records
    that go through this view are embedded within other record types.
    """

    def is_member(self, record: SourceRecord) -> FilterResult:
        """
        Filters the given record, determining whether it is an image or not.

        :param record: the record to filter
        :return: a FilterResult object
        """
        if record.get_first_value("MulMimeType") != "image":
            return MULTIMEDIA_NOT_IMAGE

        if not is_web_published(record):
            return NO_PUBLISH

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
            # TODO: this
            "identifier": f"http://10.0.11.20/media/{asset_id}",
            "title": get_first("MulTitle"),
            # TODO: maybe this should be an actual mime type? Seems to usually just be a
            #       file type like "tiff" or "jpeg"
            "mime": get_first("MulMimeFormat"),
            "creator": get_first("MulCreator"),
            "category": get_first("DetResourceType"),
            "type": "StillImage",
            "license": "http://creativecommons.org/licenses/by/4.0/",
            "rightsHolder": "The Trustees of the Natural History Museum, London",
            # TODO: new!
            "width": get_first("ChaImageWidth"),
            "height": get_first("ChaImageHeight"),
        }
