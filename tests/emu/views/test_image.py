from pathlib import Path
from typing import List, Tuple

import pytest

from dataimporter.dbs import DataDB
from dataimporter.emu.views.image import ImageView, MULTIMEDIA_NOT_IMAGE
from dataimporter.emu.views.utils import NO_PUBLISH
from dataimporter.model import SourceRecord
from dataimporter.view import FilterResult, SUCCESS_RESULT
from tests.helpers.samples.image import SAMPLE_IMAGE_DATA, SAMPLE_IMAGE_ID


@pytest.fixture
def image_view(tmp_path: Path) -> ImageView:
    view = ImageView(tmp_path / "image_view", DataDB(tmp_path / "image_data"))
    yield view
    view.close()


is_member_scenarios: List[Tuple[dict, FilterResult]] = [
    ({"MulMimeType": "Document"}, MULTIMEDIA_NOT_IMAGE),
    ({"AdmPublishWebNoPasswordFlag": "n"}, NO_PUBLISH),
    ({}, SUCCESS_RESULT),
]


@pytest.mark.parametrize("overrides, result", is_member_scenarios)
def test_is_member(overrides: dict, result: FilterResult, image_view: ImageView):
    data = {**SAMPLE_IMAGE_DATA, **overrides}
    record = SourceRecord(SAMPLE_IMAGE_ID, data, "test")
    assert image_view.is_member(record) == result


def test_transform_deleted(image_view: ImageView):
    record = SourceRecord(SAMPLE_IMAGE_ID, {}, "test")
    assert record.is_deleted

    data = image_view.transform(record)
    assert data == {}


def test_make_data(image_view: ImageView):
    record = SourceRecord(SAMPLE_IMAGE_ID, SAMPLE_IMAGE_DATA, "test")

    data = image_view.make_data(record)
    assert data == {
        "_id": record.id,
        "created": "2013-11-12T16:13:51+00:00",
        "modified": "2016-02-03T09:13:10+00:00",
        "assetID": "c2bde4e9-ca2b-466c-ab41-509468b841a4",
        "identifier": f"http://10.0.11.20/media/c2bde4e9-ca2b-466c-ab41-509468b841a4",
        "title": "BM000019319",
        "mime": "image/tiff",
        "creator": None,
        "category": "Specimen",
        "type": "StillImage",
        "license": "http://creativecommons.org/licenses/by/4.0/",
        "rightsHolder": "The Trustees of the Natural History Museum, London",
        "width": "6638",
        "height": "10199",
    }
