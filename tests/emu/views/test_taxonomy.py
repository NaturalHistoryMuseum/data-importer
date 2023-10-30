from pathlib import Path
from typing import List, Tuple

import pytest

from dataimporter.dbs import DataDB
from dataimporter.emu.views.taxonomy import TaxonomyView
from dataimporter.emu.views.utils import NO_PUBLISH
from dataimporter.model import SourceRecord
from dataimporter.view import FilterResult, SUCCESS_RESULT
from tests.helpers.samples.taxonomy import SAMPLE_TAXONOMY_DATA, SAMPLE_TAXONOMY_ID


@pytest.fixture
def taxonomy_view(tmp_path: Path) -> TaxonomyView:
    view = TaxonomyView(tmp_path / "taxonomy_view", DataDB(tmp_path / "taxonomy_data"))
    yield view
    view.close()


is_member_scenarios: List[Tuple[dict, FilterResult]] = [
    ({"AdmPublishWebNoPasswordFlag": "n"}, NO_PUBLISH),
    ({}, SUCCESS_RESULT),
]


@pytest.mark.parametrize("overrides, result", is_member_scenarios)
def test_is_member(overrides: dict, result: FilterResult, taxonomy_view: TaxonomyView):
    data = {**SAMPLE_TAXONOMY_DATA, **overrides}
    record = SourceRecord(SAMPLE_TAXONOMY_ID, data, "test")
    assert taxonomy_view.is_member(record) == result


def test_transform_deleted(taxonomy_view: TaxonomyView):
    record = SourceRecord(SAMPLE_TAXONOMY_ID, {}, "test")
    assert record.is_deleted

    data = taxonomy_view.transform(record)
    assert data == {}


def test_make_data(taxonomy_view: TaxonomyView):
    record = SourceRecord(SAMPLE_TAXONOMY_ID, SAMPLE_TAXONOMY_DATA, "test")

    data = taxonomy_view.make_data(record)
    assert data == {
        "_id": record.id,
        "created": "2007-01-16T00:00:00+00:00",
        "modified": "2021-09-29T09:04:05+00:00",
        "scientificName": "Microterys colligatus (Walker, 1872)",
        "currentScientificName": "Microterys colligatus (Walker, 1872)",
        "taxonRank": "Species",
        "kingdom": None,
        "phylum": "Arthropoda",
        "class": "Insecta",
        "order": "Hymenoptera",
        "suborder": "Parasitica",
        "superfamily": "Chalcidoidea",
        "family": "Encyrtidae",
        "subfamily": "Encyrtinae",
        "genus": "Microterys",
        "subgenus": None,
        "specificEpithet": "colligatus",
        "infraspecificEpithet": None,
    }
