from pathlib import Path
from typing import List, Tuple

import pytest

from dataimporter.dbs import DataDB
from dataimporter.emu.views.artefact import ArtefactView
from dataimporter.emu.views.utils import (
    INVALID_TYPE,
    NO_PUBLISH,
    INVALID_GUID,
    INVALID_STATUS,
    INVALID_DEPARTMENT,
)
from dataimporter.model import SourceRecord
from dataimporter.view import FilterResult, SUCCESS_RESULT
from tests.helpers.samples.artefact import SAMPLE_ARTEFACT_DATA, SAMPLE_ARTEFACT_ID


@pytest.fixture
def artefact_view(tmp_path: Path) -> ArtefactView:
    view = ArtefactView(tmp_path / "artefact_view", DataDB(tmp_path / "artefact_data"))
    yield view
    view.close()


is_member_scenarios: List[Tuple[dict, FilterResult]] = [
    ({"ColRecordType": "Specimen"}, INVALID_TYPE),
    ({"AdmPublishWebNoPasswordFlag": "n"}, NO_PUBLISH),
    ({"AdmGUIDPreferredValue": "not a valid guid!"}, INVALID_GUID),
    ({"SecRecordStatus": "INVALID"}, INVALID_STATUS),
    ({"ColDepartment": "DDI"}, INVALID_DEPARTMENT),
    ({}, SUCCESS_RESULT),
]


@pytest.mark.parametrize("overrides, result", is_member_scenarios)
def test_is_member(overrides: dict, result: FilterResult, artefact_view: ArtefactView):
    data = {**SAMPLE_ARTEFACT_DATA, **overrides}
    record = SourceRecord(SAMPLE_ARTEFACT_ID, data, "test")
    assert artefact_view.is_member(record) == result


def test_transform_deleted(artefact_view: ArtefactView):
    record = SourceRecord(SAMPLE_ARTEFACT_ID, {}, "test")
    assert record.is_deleted

    data = artefact_view.transform(record)
    assert data == {}


def test_make_data(artefact_view: ArtefactView):
    record = SourceRecord(SAMPLE_ARTEFACT_ID, SAMPLE_ARTEFACT_DATA, "test")

    data = artefact_view.make_data(record)
    assert data == {
        "_id": record.id,
        "created": "2012-10-25T11:10:57+00:00",
        "modified": "2017-04-09T16:12:10+00:00",
        "artefactName": "Argonauta argo, male, Ward No. 550",
        "artefactType": None,
        "artefactDescription": "Blaschka glass model of mollusc - Argonaut",
        "scientificName": "Argonauta argo (L., 1758)",
    }
