from typing import List, Tuple

import pytest

from dataimporter.emu.views.threed import (
    INVALID_PUBLISHER,
    MULTIMEDIA_NOT_URL,
    NOT_SPECIMEN,
    ThreeDView,
)
from dataimporter.emu.views.utils import INVALID_GUID, NO_PUBLISH
from dataimporter.lib.model import SourceRecord
from dataimporter.lib.view import SUCCESS_RESULT, FilterResult
from tests.helpers.samples.threed import (
    MS_SAMPLE_3D_DATA,
    MS_SAMPLE_3D_ID,
    SF_SAMPLE_3D_DATA,
    SF_SAMPLE_3D_ID,
)
from tests.helpers.utils import is_member_error, is_publishable_error

is_publishable_member_scenarios: List[
    Tuple[dict, FilterResult, FilterResult, FilterResult]
] = [
    ({'MulDocumentType': 'I'}, *is_member_error(MULTIMEDIA_NOT_URL)),
    (
        {'AdmGUIDPreferredValue': 'not a valid guid!'},
        *is_publishable_error(INVALID_GUID),
    ),
    ({'AdmPublishWebNoPasswordFlag': 'n'}, *is_publishable_error(NO_PUBLISH)),
    ({'DetPublisher': 'banana'}, *is_member_error(INVALID_PUBLISHER)),
    ({'DetResourceType': 'Document'}, *is_member_error(NOT_SPECIMEN)),
    ({}, SUCCESS_RESULT, SUCCESS_RESULT, SUCCESS_RESULT),
]


@pytest.mark.parametrize(
    'overrides, member_result, publishable_result, publishable_member_result',
    is_publishable_member_scenarios,
)
def test_is_publishable_member(
    overrides: dict,
    member_result: FilterResult,
    publishable_result: FilterResult,
    publishable_member_result: FilterResult,
    three_d_view: ThreeDView,
):
    # check with a sketchfab sample
    sf_data = {**SF_SAMPLE_3D_DATA, **overrides}
    sf_record = SourceRecord(SF_SAMPLE_3D_ID, sf_data, 'test')
    assert three_d_view.is_member(sf_record) == member_result
    assert three_d_view.is_publishable(sf_record) == publishable_result
    assert three_d_view.is_publishable_member(sf_record) == publishable_member_result

    # check with a morphosource sample
    ms_data = {**MS_SAMPLE_3D_DATA, **overrides}
    ms_record = SourceRecord(MS_SAMPLE_3D_ID, ms_data, 'test')
    assert three_d_view.is_member(ms_record) == member_result
    assert three_d_view.is_publishable(ms_record) == publishable_result
    assert three_d_view.is_publishable_member(ms_record) == publishable_member_result


def test_transform_sketchfab(three_d_view: ThreeDView):
    record = SourceRecord(SF_SAMPLE_3D_ID, SF_SAMPLE_3D_DATA, 'test')

    data = three_d_view.transform(record)
    assert data == {
        '_id': record.id,
        'created': '2021-11-16T16:00:40+00:00',
        'modified': '2021-11-16T16:02:53+00:00',
        'assetID': '5ab7511b-8999-4181-b1ba-c843bc50b3d5',
        'identifier': 'https://skfb.ly/oqKBO',
        'title': '3D Visual of PV M 82206 C (a) on Sketchfab',
        'creator': 'NHM  Image Resources',
        'category': 'Specimen',
        'type': '3D',
        'license': 'http://creativecommons.org/licenses/by/4.0/',
        'rightsHolder': 'The Trustees of the Natural History Museum, London',
    }


def test_transform_morphosource(three_d_view: ThreeDView):
    record = SourceRecord(MS_SAMPLE_3D_ID, MS_SAMPLE_3D_DATA, 'test')

    data = three_d_view.transform(record)
    assert data == {
        '_id': record.id,
        'created': '2021-10-12T13:56:54+00:00',
        'modified': '2023-03-15T14:16:04+00:00',
        'assetID': '47b4d810-fb4f-48f9-bc31-8fe2b25a232c',
        'identifier': 'https://www.morphosource.org/concern/media/000388716?locale=en',
        'title': '3D meshfile of CT scan of TB 14675 Venericor sp.',
        'creator': 'Katie Collins',
        'category': 'Specimen',
        'type': '3D',
        'license': 'http://creativecommons.org/licenses/by/4.0/',
        'rightsHolder': 'The Trustees of the Natural History Museum, London',
    }
