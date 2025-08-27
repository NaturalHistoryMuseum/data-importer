from typing import List, Tuple

import pytest

from dataimporter.emu.views.image import MULTIMEDIA_NOT_IMAGE, ImageView
from dataimporter.emu.views.utils import INVALID_GUID, NO_PUBLISH
from dataimporter.lib.model import SourceRecord
from dataimporter.lib.view import SUCCESS_RESULT, FilterResult
from tests.helpers.samples.image import SAMPLE_IMAGE_DATA, SAMPLE_IMAGE_ID
from tests.helpers.utils import is_member_error, is_publishable_error

is_publishable_member_scenarios: List[
    Tuple[dict, FilterResult, FilterResult, FilterResult]
] = [
    ({'MulMimeType': 'Document'}, *is_member_error(MULTIMEDIA_NOT_IMAGE)),
    (
        {'AdmGUIDPreferredValue': 'not a valid guid!'},
        *is_publishable_error(INVALID_GUID),
    ),
    ({'AdmPublishWebNoPasswordFlag': 'n'}, *is_publishable_error(NO_PUBLISH)),
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
    image_view: ImageView,
):
    data = {**SAMPLE_IMAGE_DATA, **overrides}
    record = SourceRecord(SAMPLE_IMAGE_ID, data, 'test')
    assert image_view.is_member(record) == member_result
    assert image_view.is_publishable(record) == publishable_result
    assert image_view.is_publishable_member(record) == publishable_member_result


def test_transform(image_view: ImageView):
    record = SourceRecord(SAMPLE_IMAGE_ID, SAMPLE_IMAGE_DATA, 'test')

    data = image_view.transform(record)
    assert data == {
        '_id': record.id,
        'created': '2013-11-12T16:13:51+00:00',
        'modified': '2016-02-03T09:13:10+00:00',
        'assetID': 'c2bde4e9-ca2b-466c-ab41-509468b841a4',
        'identifier': f'{image_view.iiif_url_base}/c2bde4e9-ca2b-466c-ab41-509468b841a4',
        'title': 'BM000019319',
        'format': 'image/tiff',
        'category': 'Specimen',
        'type': 'StillImage',
        'license': 'http://creativecommons.org/licenses/by/4.0/',
        'rightsHolder': 'The Trustees of the Natural History Museum, London',
        'PixelXDimension': '6638',
        'PixelYDimension': '10199',
    }


def test_transform_with_orientation(image_view: ImageView):
    record_data = SAMPLE_IMAGE_DATA.copy()

    # replace the orientation value with one that requires a width/height swap
    values = list(record_data['ExiValue'])
    values[8] = 'Mirror horizontal and rotate 270 CW'
    record_data['ExiValue'] = tuple(values)

    record = SourceRecord(SAMPLE_IMAGE_ID, record_data, 'test')

    data = image_view.transform(record)
    assert data == {
        '_id': record.id,
        'created': '2013-11-12T16:13:51+00:00',
        'modified': '2016-02-03T09:13:10+00:00',
        'assetID': 'c2bde4e9-ca2b-466c-ab41-509468b841a4',
        'identifier': f'{image_view.iiif_url_base}/c2bde4e9-ca2b-466c-ab41-509468b841a4',
        'title': 'BM000019319',
        'format': 'image/tiff',
        'category': 'Specimen',
        'type': 'StillImage',
        'license': 'http://creativecommons.org/licenses/by/4.0/',
        'rightsHolder': 'The Trustees of the Natural History Museum, London',
        'PixelXDimension': '10199',
        'PixelYDimension': '6638',
    }
