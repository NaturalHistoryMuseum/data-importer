from typing import List, Tuple

import pytest

from dataimporter.emu.views.taxonomy import TaxonomyView
from dataimporter.emu.views.utils import NO_PUBLISH
from dataimporter.lib.model import SourceRecord
from dataimporter.lib.view import SUCCESS_RESULT, FilterResult
from tests.helpers.samples.taxonomy import SAMPLE_TAXONOMY_DATA, SAMPLE_TAXONOMY_ID

is_member_scenarios: List[Tuple[dict, FilterResult]] = [
    ({'AdmPublishWebNoPasswordFlag': 'n'}, NO_PUBLISH),
    ({}, SUCCESS_RESULT),
]


@pytest.mark.parametrize('overrides, result', is_member_scenarios)
def test_is_member(overrides: dict, result: FilterResult, taxonomy_view: TaxonomyView):
    data = {**SAMPLE_TAXONOMY_DATA, **overrides}
    record = SourceRecord(SAMPLE_TAXONOMY_ID, data, 'test')
    assert taxonomy_view.is_member(record) == result


def test_make_data(taxonomy_view: TaxonomyView):
    record = SourceRecord(SAMPLE_TAXONOMY_ID, SAMPLE_TAXONOMY_DATA, 'test')

    data = taxonomy_view.transform(record)
    assert data == {
        '_id': record.id,
        'created': '2007-01-16T00:00:00+00:00',
        'modified': '2021-09-29T09:04:05+00:00',
        'scientificName': 'Microterys colligatus (Walker, 1872)',
        'currentScientificName': 'Microterys colligatus (Walker, 1872)',
        'taxonRank': 'Species',
        'phylum': 'Arthropoda',
        'class': 'Insecta',
        'order': 'Hymenoptera',
        'suborder': 'Parasitica',
        'superfamily': 'Chalcidoidea',
        'family': 'Encyrtidae',
        'subfamily': 'Encyrtinae',
        'genus': 'Microterys',
        'specificEpithet': 'colligatus',
    }
