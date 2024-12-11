from typing import List, Tuple

import pytest

from dataimporter.emu.views.mss import (
    MSSView,
    MULTIMEDIA_NOT_IMAGE,
    MULTIMEDIA_NO_IDENTIFIER,
)
from dataimporter.emu.views.utils import NO_PUBLISH, INVALID_GUID
from dataimporter.lib.model import SourceRecord
from dataimporter.lib.view import FilterResult, SUCCESS_RESULT
from tests.helpers.samples.image import SAMPLE_IMAGE_DATA, SAMPLE_IMAGE_ID

is_member_scenarios: List[Tuple[dict, FilterResult]] = [
    ({"MulMimeType": "Document"}, MULTIMEDIA_NOT_IMAGE),
    ({"AdmGUIDPreferredValue": "not a valid guid!"}, INVALID_GUID),
    ({"AdmPublishWebNoPasswordFlag": "n"}, NO_PUBLISH),
    ({"DocIdentifier": None}, MULTIMEDIA_NO_IDENTIFIER),
    ({}, SUCCESS_RESULT),
]


@pytest.mark.parametrize("overrides, result", is_member_scenarios)
def test_is_member(overrides: dict, result: FilterResult, mss_view: MSSView):
    data = {**SAMPLE_IMAGE_DATA, **overrides}
    record = SourceRecord(SAMPLE_IMAGE_ID, data, "test")
    assert mss_view.is_member(record) == result


def test_transform(mss_view: MSSView):
    record = SourceRecord(SAMPLE_IMAGE_ID, SAMPLE_IMAGE_DATA, "test")

    data = mss_view.transform(record)
    assert data == {
        "id": record.id,
        "mime": "tiff",
        "guid": "c2bde4e9-ca2b-466c-ab41-509468b841a4",
        "file": "BM000019319.tif",
        "width": 6638,
        "height": 10199,
        "orientation": "Horizontal (normal)",
        "old_asset_id": "0d5f124013467e40307c6e0dc7595cd92d25b907",
        "derivatives": [
            {"file": "BM000019319.thumb.jpg", "width": 59, "height": 90},
            {"file": "BM000019319.120x10199.jpeg", "width": 120, "height": 184},
            {"file": "BM000019319.200x10199.jpeg", "width": 200, "height": 307},
            {"file": "BM000019319.325x10199.jpeg", "width": 325, "height": 499},
            {"file": "BM000019319.470x10199.jpeg", "width": 470, "height": 722},
            {"file": "BM000019319.705x10199.jpeg", "width": 705, "height": 1083},
            {"file": "BM000019319.1500x10199.jpeg", "width": 1500, "height": 2305},
        ],
    }


def test_transform_no_derivatives(mss_view: MSSView):
    data = SAMPLE_IMAGE_DATA.copy()
    data["DocIdentifier"] = data["DocIdentifier"][0]
    data["DocWidth"] = data["DocWidth"][0]
    data["DocHeight"] = data["DocHeight"][0]
    record = SourceRecord(SAMPLE_IMAGE_ID, data, "test")

    data = mss_view.transform(record)
    assert data == {
        "id": record.id,
        "mime": "tiff",
        "guid": "c2bde4e9-ca2b-466c-ab41-509468b841a4",
        "file": "BM000019319.tif",
        "width": 6638,
        "height": 10199,
        "old_asset_id": "0d5f124013467e40307c6e0dc7595cd92d25b907",
        "orientation": "Horizontal (normal)",
    }


def test_transform_no_orientation(mss_view: MSSView):
    data = SAMPLE_IMAGE_DATA.copy()
    # remove the exif tag in our sample data which stores the orientation value (and all
    # the associated tag and tag name bits EMu exports)
    for field in ["ExiTag", "ExiName", "ExiValue"]:
        # in the test data the orientation tag is the 8th element of the exif lists
        data[field] = tuple(v for i, v in enumerate(data[field]) if i != 8)
    record = SourceRecord(SAMPLE_IMAGE_ID, data, "test")

    data = mss_view.transform(record)
    assert "orientation" not in data
