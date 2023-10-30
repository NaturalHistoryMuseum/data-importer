from pathlib import Path
from typing import List, Tuple

import pytest

from dataimporter.dbs import DataDB
from dataimporter.emu.views.indexlot import IndexLotView
from dataimporter.emu.views.utils import (
    INVALID_TYPE,
    NO_PUBLISH,
    INVALID_GUID,
    INVALID_STATUS,
    INVALID_DEPARTMENT,
)
from dataimporter.model import SourceRecord
from dataimporter.view import FilterResult, SUCCESS_RESULT
from tests.helpers.samples.indexlot import SAMPLE_INDEXLOT_ID, SAMPLE_INDEXLOT_DATA


@pytest.fixture
def indexlot_view(tmp_path: Path) -> IndexLotView:
    view = IndexLotView(tmp_path / "indexlot_view", DataDB(tmp_path / "indexlot_data"))
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
def test_is_member(overrides: dict, result: FilterResult, indexlot_view: IndexLotView):
    data = {**SAMPLE_INDEXLOT_DATA, **overrides}
    record = SourceRecord(SAMPLE_INDEXLOT_ID, data, "test")
    assert indexlot_view.is_member(record) == result


def test_transform_deleted(indexlot_view: IndexLotView):
    record = SourceRecord(SAMPLE_INDEXLOT_ID, {}, "test")
    assert record.is_deleted

    data = indexlot_view.transform(record)
    assert data == {}


def test_make_data(indexlot_view: IndexLotView):
    record = SourceRecord(SAMPLE_INDEXLOT_ID, SAMPLE_INDEXLOT_DATA, "test")

    data = indexlot_view.make_data(record)
    assert data == {
        "_id": record.id,
        "created": "1999-07-13T00:00:00+00:00",
        "modified": "2023-10-05T16:41:54+00:00",
        "british": "No",
        "kindOfMaterial": "Dry",
        "kindOfMedia": None,
        "material": "Yes",
        "materialCount": "1",
        "materialPrimaryTypeNumber": None,
        "materialSex": None,
        "materialStage": None,
        "materialTypes": "Type",
        "media": None,
        "type": "Yes",
    }
