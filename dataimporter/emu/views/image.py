from pathlib import Path

from dataimporter.emu.views.utils import (
    NO_PUBLISH,
    is_web_published,
    is_valid_guid,
    INVALID_GUID,
    orientation_requires_swap,
)
from dataimporter.emu.views.utils import emu_date
from dataimporter.lib.dbs import Store
from dataimporter.lib.model import SourceRecord
from dataimporter.lib.view import FilterResult, View, SUCCESS_RESULT, strip_empty

MULTIMEDIA_NOT_IMAGE = FilterResult(False, "Multimedia not an image")


class ImageView(View):
    """
    View for image records.

    This view doesn't have a Data Portal resource that it populates, instead the records
    that go through this view are embedded within other record types.
    """

    def __init__(self, path: Path, store: Store, iiif_url_base: str):
        """
        :param path: the root path that all view related data should be stored under
        :param db: the DataDB object that backs this view
        :param iiif_url_base: base URL for the IIIF image URLs created in make_data
        """
        super().__init__(path, store)
        self.iiif_url_base = iiif_url_base

    def is_member(self, record: SourceRecord) -> FilterResult:
        """
        Filters the given record, determining whether it is an image or not.

        :param record: the record to filter
        :return: a FilterResult object
        """
        if record.get_first_value("MulMimeType") != "image":
            return MULTIMEDIA_NOT_IMAGE

        if not is_valid_guid(record):
            return INVALID_GUID

        if not is_web_published(record):
            return NO_PUBLISH

        return SUCCESS_RESULT

    @strip_empty
    def transform(self, record: SourceRecord) -> dict:
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
        data = {
            "_id": record.id,
            "created": emu_date(
                get_first("AdmDateInserted"), get_first("AdmTimeInserted")
            ),
            "modified": emu_date(
                get_first("AdmDateModified"), get_first("AdmTimeModified")
            ),
            "assetID": asset_id,
            "identifier": f"{self.iiif_url_base}/{asset_id}",
            "title": get_first("MulTitle"),
            "creator": get_first("MulCreator"),
            "category": get_first("DetResourceType"),
            "type": "StillImage",
            "license": "http://creativecommons.org/licenses/by/4.0/",
            "rightsHolder": "The Trustees of the Natural History Museum, London",
        }

        width = get_first("ChaImageWidth")
        height = get_first("ChaImageHeight")
        if width and height:
            swap = orientation_requires_swap(record)
            data["PixelXDimension"] = width if not swap else height
            data["PixelYDimension"] = height if not swap else width

        if mime_subtype := get_first("MulMimeFormat"):
            # we know that the mime type is image because it's in our member filter
            data["format"] = f"image/{mime_subtype}"

        return data
