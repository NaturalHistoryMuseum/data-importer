from typing import List, Tuple

import pytest

from dataimporter.emu.views.image import ImageView
from dataimporter.emu.views.indexlot import IndexLotView
from dataimporter.emu.views.taxonomy import TaxonomyView
from dataimporter.emu.views.utils import (
    INVALID_DEPARTMENT,
    INVALID_GUID,
    INVALID_STATUS,
    INVALID_TYPE,
    NO_PUBLISH,
)
from dataimporter.lib.model import SourceRecord
from dataimporter.lib.view import SUCCESS_RESULT, FilterResult
from tests.helpers.samples.image import SAMPLE_IMAGE_DATA, SAMPLE_IMAGE_ID
from tests.helpers.samples.indexlot import SAMPLE_INDEXLOT_DATA, SAMPLE_INDEXLOT_ID
from tests.helpers.samples.taxonomy import SAMPLE_TAXONOMY_DATA, SAMPLE_TAXONOMY_ID
from tests.helpers.utils import is_member_error, is_publishable_error

is_publishable_member_scenarios: List[
    Tuple[dict, FilterResult, FilterResult, FilterResult]
] = [
    ({'ColRecordType': 'Specimen'}, *is_member_error(INVALID_TYPE)),
    ({'ColDepartment': 'DDI'}, *is_member_error(INVALID_DEPARTMENT)),
    ({'AdmPublishWebNoPasswordFlag': 'n'}, *is_publishable_error(NO_PUBLISH)),
    (
        {'AdmGUIDPreferredValue': 'not a valid guid!'},
        *is_publishable_error(INVALID_GUID),
    ),
    ({'SecRecordStatus': 'INVALID'}, *is_publishable_error(INVALID_STATUS)),
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
    indexlot_view: IndexLotView,
):
    data = {**SAMPLE_INDEXLOT_DATA, **overrides}
    record = SourceRecord(SAMPLE_INDEXLOT_ID, data, 'test')
    assert indexlot_view.is_member(record) == member_result
    assert indexlot_view.is_publishable(record) == publishable_result
    assert indexlot_view.is_publishable_member(record) == publishable_member_result


def test_transform_no_images_no_taxonomy(indexlot_view: IndexLotView):
    record = SourceRecord(SAMPLE_INDEXLOT_ID, SAMPLE_INDEXLOT_DATA, 'test')

    data = indexlot_view.transform(record)
    assert data == {
        '_id': record.id,
        'created': '1999-07-13T00:00:00+00:00',
        'modified': '2023-10-05T16:41:54+00:00',
        'british': 'No',
        'kindOfMaterial': 'Dry',
        'material': 'Yes',
        'materialCount': '1',
        'materialTypes': 'Type',
        'type': 'Yes',
    }


def test_transform_with_images(indexlot_view: IndexLotView, image_view: ImageView):
    record = SourceRecord(SAMPLE_INDEXLOT_ID, SAMPLE_INDEXLOT_DATA, 'test')

    # add an image to the image view's store
    image_record = SourceRecord(SAMPLE_IMAGE_ID, SAMPLE_IMAGE_DATA, 'test')
    image_view.store.put([image_record])

    data = indexlot_view.transform(record)
    assert data == {
        '_id': record.id,
        'created': '1999-07-13T00:00:00+00:00',
        'modified': '2023-10-05T16:41:54+00:00',
        'british': 'No',
        'kindOfMaterial': 'Dry',
        'material': 'Yes',
        'materialCount': '1',
        'materialTypes': 'Type',
        'type': 'Yes',
        'associatedMedia': [image_view.transform(image_record)],
        'associatedMediaCount': 1,
    }


def test_transform_with_taxonomy(
    indexlot_view: IndexLotView, taxonomy_view: TaxonomyView
):
    record = SourceRecord(SAMPLE_INDEXLOT_ID, SAMPLE_INDEXLOT_DATA, 'test')

    # add an image to the image view's store
    taxonomy_record = SourceRecord(SAMPLE_TAXONOMY_ID, SAMPLE_TAXONOMY_DATA, 'test')
    taxonomy_view.store.put([taxonomy_record])

    data = indexlot_view.transform(record)
    taxonomy_data = taxonomy_view.transform(taxonomy_record)
    expected_data = {
        '_id': record.id,
        'created': '1999-07-13T00:00:00+00:00',
        'modified': '2023-10-05T16:41:54+00:00',
        'british': 'No',
        'kindOfMaterial': 'Dry',
        'material': 'Yes',
        'materialCount': '1',
        'materialTypes': 'Type',
        'type': 'Yes',
    }
    expected_data.update(
        (key, value) for key, value in taxonomy_data.items() if key not in expected_data
    )
    assert data == expected_data
