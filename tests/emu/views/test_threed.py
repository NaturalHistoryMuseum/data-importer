from typing import List, Tuple

import pytest

from dataimporter.emu.views.threed import (
    ThreeDView,
    MULTIMEDIA_NOT_URL,
    INVALID_PUBLISHER,
    NOT_SPECIMEN,
)
from dataimporter.emu.views.utils import NO_PUBLISH, INVALID_GUID
from dataimporter.lib.model import SourceRecord
from dataimporter.lib.view import FilterResult, SUCCESS_RESULT
from tests.helpers.samples.threed import (
    SF_SAMPLE_3D_ID,
    SF_SAMPLE_3D_DATA,
    MS_SAMPLE_3D_ID,
    MS_SAMPLE_3D_DATA,
)

is_member_scenarios: List[Tuple[dict, FilterResult]] = [
    ({"MulDocumentType": "I"}, MULTIMEDIA_NOT_URL),
    ({"AdmGUIDPreferredValue": "not a valid guid!"}, INVALID_GUID),
    ({"AdmPublishWebNoPasswordFlag": "n"}, NO_PUBLISH),
    ({"DetPublisher": "banana"}, INVALID_PUBLISHER),
    ({"DetResourceType": "Document"}, NOT_SPECIMEN),
    ({}, SUCCESS_RESULT),
]


@pytest.mark.parametrize("overrides, result", is_member_scenarios)
def test_is_member(overrides: dict, result: FilterResult, three_d_view: ThreeDView):
    # check with a sketchfab sample
    sf_data = {**SF_SAMPLE_3D_DATA, **overrides}
    sf_record = SourceRecord(SF_SAMPLE_3D_ID, sf_data, "test")
    assert three_d_view.is_member(sf_record) == result

    # check with a morphosource sample
    ms_data = {**MS_SAMPLE_3D_DATA, **overrides}
    ms_record = SourceRecord(MS_SAMPLE_3D_ID, ms_data, "test")
    assert three_d_view.is_member(ms_record) == result


def test_transform_sketchfab(three_d_view: ThreeDView):
    record = SourceRecord(SF_SAMPLE_3D_ID, SF_SAMPLE_3D_DATA, "test")

    data = three_d_view.transform(record)
    assert data == {
        "_id": record.id,
        "created": "2021-11-16T16:00:40+00:00",
        "modified": "2021-11-16T16:02:53+00:00",
        "assetID": "5ab7511b-8999-4181-b1ba-c843bc50b3d5",
        "identifier": "https://skfb.ly/oqKBO",
        "title": "3D Visual of PV M 82206 C (a) on Sketchfab",
        "creator": "NHM  Image Resources",
        "category": "Specimen",
        "type": "3D",
        "license": "http://creativecommons.org/licenses/by/4.0/",
        "rightsHolder": "The Trustees of the Natural History Museum, London",
    }


def test_transform_morphosource(three_d_view: ThreeDView):
    record = SourceRecord(MS_SAMPLE_3D_ID, MS_SAMPLE_3D_DATA, "test")

    data = three_d_view.transform(record)
    assert data == {
        "_id": record.id,
        "created": "2021-10-12T13:56:54+00:00",
        "modified": "2023-03-15T14:16:04+00:00",
        "assetID": "47b4d810-fb4f-48f9-bc31-8fe2b25a232c",
        "identifier": "https://www.morphosource.org/concern/media/000388716?locale=en",
        "title": "3D meshfile of CT scan of TB 14675 Venericor sp.",
        "creator": "Katie Collins",
        "category": "Specimen",
        "type": "3D",
        "license": "http://creativecommons.org/licenses/by/4.0/",
        "rightsHolder": "The Trustees of the Natural History Museum, London",
    }
