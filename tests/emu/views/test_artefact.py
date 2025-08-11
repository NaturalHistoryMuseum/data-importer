from typing import List, Tuple

import pytest

from dataimporter.emu.views.artefact import ArtefactView
from dataimporter.emu.views.image import ImageView
from dataimporter.emu.views.utils import (
    INVALID_DEPARTMENT,
    INVALID_GUID,
    INVALID_STATUS,
    INVALID_TYPE,
    NO_PUBLISH,
)
from dataimporter.lib.model import SourceRecord
from dataimporter.lib.view import SUCCESS_RESULT, FilterResult
from tests.helpers.samples.artefact import SAMPLE_ARTEFACT_DATA, SAMPLE_ARTEFACT_ID
from tests.helpers.samples.image import SAMPLE_IMAGE_DATA, SAMPLE_IMAGE_ID

is_member_scenarios: List[Tuple[dict, FilterResult]] = [
    ({'ColRecordType': 'Specimen'}, INVALID_TYPE),
    ({'AdmPublishWebNoPasswordFlag': 'n'}, NO_PUBLISH),
    ({'AdmGUIDPreferredValue': 'not a valid guid!'}, INVALID_GUID),
    ({'SecRecordStatus': 'INVALID'}, INVALID_STATUS),
    ({'ColDepartment': 'DDI'}, INVALID_DEPARTMENT),
    ({}, SUCCESS_RESULT),
]


@pytest.mark.parametrize('overrides, result', is_member_scenarios)
def test_is_member(overrides: dict, result: FilterResult, artefact_view: ArtefactView):
    data = {**SAMPLE_ARTEFACT_DATA, **overrides}
    record = SourceRecord(SAMPLE_ARTEFACT_ID, data, 'test')
    assert artefact_view.is_member(record) == result


def test_transform_no_images(artefact_view: ArtefactView):
    record = SourceRecord(SAMPLE_ARTEFACT_ID, SAMPLE_ARTEFACT_DATA, 'test')

    data = artefact_view.transform(record)
    assert data == {
        '_id': record.id,
        'created': '2012-10-25T11:10:57+00:00',
        'modified': '2017-04-09T16:12:10+00:00',
        'artefactName': 'Argonauta argo, male, Ward No. 550',
        'artefactDescription': 'Blaschka glass model of mollusc - Argonaut',
        'scientificName': 'Argonauta argo (L., 1758)',
    }


def test_transform_with_images(artefact_view: ArtefactView, image_view: ImageView):
    record = SourceRecord(SAMPLE_ARTEFACT_ID, SAMPLE_ARTEFACT_DATA, 'test')

    # add an image to the image view's store
    image_record = SourceRecord(SAMPLE_IMAGE_ID, SAMPLE_IMAGE_DATA, 'test')
    image_view.store.put([image_record])

    data = artefact_view.transform(record)
    assert data == {
        '_id': record.id,
        'created': '2012-10-25T11:10:57+00:00',
        'modified': '2017-04-09T16:12:10+00:00',
        'artefactName': 'Argonauta argo, male, Ward No. 550',
        'artefactDescription': 'Blaschka glass model of mollusc - Argonaut',
        'scientificName': 'Argonauta argo (L., 1758)',
        'associatedMedia': [image_view.transform(image_record)],
        'associatedMediaCount': 1,
    }
