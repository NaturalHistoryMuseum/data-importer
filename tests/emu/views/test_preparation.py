from contextlib import closing
from pathlib import Path
from typing import List, Tuple

import pytest

from dataimporter.emu.views.preparation import (
    PreparationView,
    INVALID_SUB_DEPARTMENT,
    INVALID_PROJECT,
    get_preparation_process,
    is_on_loan,
    ON_LOAN,
)
from dataimporter.emu.views.utils import (
    NO_PUBLISH,
    INVALID_TYPE,
    INVALID_GUID,
    INVALID_STATUS,
    INVALID_DEPARTMENT,
)
from dataimporter.lib.dbs import DataDB
from dataimporter.lib.model import SourceRecord
from dataimporter.lib.view import FilterResult, SUCCESS_RESULT
from tests.helpers.samples.preparation import (
    SAMPLE_PREPARATION_DATA,
    SAMPLE_PREPARATION_ID,
    SAMPLE_MAMMAL_PREPARATION_ID,
    SAMPLE_MAMMAL_PREPARATION_DATA,
)


@pytest.fixture
def prep_view(tmp_path: Path) -> PreparationView:
    with closing(
        PreparationView(tmp_path / "prep_view", DataDB(tmp_path / "prep_data"))
    ) as view:
        yield view


mol_prep_is_member_scenarios: List[Tuple[dict, FilterResult]] = [
    ({"ColRecordType": "Specimen"}, INVALID_TYPE),
    # this is a check to make sure a mammal part in molecular collections doesn't come
    # through
    ({"ColRecordType": "Mammal Group Part"}, INVALID_SUB_DEPARTMENT),
    ({"AdmPublishWebNoPasswordFlag": "n"}, NO_PUBLISH),
    ({"AdmGUIDPreferredValue": "not a valid guid!"}, INVALID_GUID),
    ({"SecRecordStatus": "INVALID"}, INVALID_STATUS),
    ({"ColDepartment": "DDI"}, INVALID_DEPARTMENT),
    ({"ColSubDepartment": "Informatics"}, INVALID_SUB_DEPARTMENT),
    ({"ColSubDepartment": "LS Mammals"}, INVALID_SUB_DEPARTMENT),
    ({"LocPermanentLocationRef": "3250522"}, ON_LOAN),
    ({"LocCurrentSummaryData": "ON LOAN"}, ON_LOAN),
    ({}, SUCCESS_RESULT),
]


@pytest.mark.parametrize("overrides, result", mol_prep_is_member_scenarios)
def test_is_member_mol_prep(
    overrides: dict, result: FilterResult, prep_view: PreparationView
):
    data = {**SAMPLE_PREPARATION_DATA, **overrides}
    record = SourceRecord(SAMPLE_PREPARATION_ID, data, "test")
    assert prep_view.is_member(record) == result


mammal_part_prep_is_member_scenarios: List[Tuple[dict, FilterResult]] = [
    ({"ColRecordType": "Specimen"}, INVALID_TYPE),
    ({"AdmPublishWebNoPasswordFlag": "n"}, NO_PUBLISH),
    ({"AdmGUIDPreferredValue": "not a valid guid!"}, INVALID_GUID),
    ({"SecRecordStatus": "INVALID"}, INVALID_STATUS),
    ({"ColDepartment": "DDI"}, INVALID_DEPARTMENT),
    ({"ColSubDepartment": "Informatics"}, INVALID_SUB_DEPARTMENT),
    ({"ColSubDepartment": "Molecular Collections"}, INVALID_SUB_DEPARTMENT),
    ({"NhmSecProjectName": "Life of Darwin Tree"}, INVALID_PROJECT),
    # this is a check to make sure a prep in LS Mammals doesn't come through
    ({"ColRecordType": "Preparation"}, INVALID_SUB_DEPARTMENT),
    ({"LocPermanentLocationRef": "3250522"}, ON_LOAN),
    ({"LocCurrentSummaryData": "ON LOAN"}, ON_LOAN),
    ({}, SUCCESS_RESULT),
]


@pytest.mark.parametrize("overrides, result", mammal_part_prep_is_member_scenarios)
def test_is_member_mammal_part_prep(
    overrides: dict, result: FilterResult, prep_view: PreparationView
):
    data = {**SAMPLE_MAMMAL_PREPARATION_DATA, **overrides}
    record = SourceRecord(SAMPLE_MAMMAL_PREPARATION_ID, data, "test")
    assert prep_view.is_member(record) == result


def test_transform_deleted(prep_view: PreparationView):
    record = SourceRecord("an_ID_it_does_not_matter", {}, "test")
    assert record.is_deleted

    data = prep_view.transform(record)
    assert data == {}


def test_make_data_mol_prep(prep_view: PreparationView):
    record = SourceRecord(SAMPLE_PREPARATION_ID, SAMPLE_PREPARATION_DATA, "test")

    data = prep_view.make_data(record)
    assert data == {
        "_id": record.id,
        "created": "2022-09-12T17:07:51+00:00",
        "modified": "2022-09-12T17:21:14+00:00",
        "project": "Darwin Tree of Life",
        "preparationNumber": "C9K02TWP_B2",
        "preparationType": "DNA Extract",
        "mediumType": None,
        "preparationProcess": None,
        "preparationContents": "**OTHER_SOMATIC_ANIMAL_TISSUE**",
        "preparationDate": "2022-05-09",
    }


def test_make_data_mammal_part(prep_view: PreparationView):
    record = SourceRecord(
        SAMPLE_MAMMAL_PREPARATION_ID, SAMPLE_MAMMAL_PREPARATION_DATA, "test"
    )

    data = prep_view.make_data(record)
    assert data == {
        "_id": record.id,
        "created": "2023-05-02T14:55:51+00:00",
        "modified": "2023-05-02T14:55:51+00:00",
        "project": "Darwin Tree of Life",
        "preparationNumber": "FF06063966",
        "preparationType": None,
        "mediumType": None,
        "preparationProcess": "Flash Freezing: Dry Ice",
        "preparationContents": "MUSCLE",
        "preparationDate": None,
    }


process_cleaning_scenarios = [
    # these two shouldn't happen but might as well check
    (None, None),
    ("", None),
    ("100% ethanol", "100% ethanol"),
    # lowercase, all variants
    ("killing agent: 100% ethanol", "100% ethanol"),
    ("killing agent:     100% ethanol", "100% ethanol"),
    ("killing agent 100% ethanol", "100% ethanol"),
    ("killing agent     100% ethanol", "100% ethanol"),
    # mixed case, all variants
    ("kilLing AgenT: 100% ethanol", "100% ethanol"),
    ("kilLing AgenT:      100% ethanol", "100% ethanol"),
    ("kilLing AgenT 100% ethanol", "100% ethanol"),
    ("kilLing AgenT       100% ethanol", "100% ethanol"),
    # killing agent mentioned more than once
    ("killing agent: plant killing agent", "plant killing agent"),
]


class TestGetPreparationProcess:
    def test_missing(self):
        record = SourceRecord("t1", {"oh": "no!"}, "test")
        assert get_preparation_process(record) is None

    @pytest.mark.parametrize("value,clean_value", process_cleaning_scenarios)
    def test_clean(self, value, clean_value):
        record = SourceRecord("t1", {"EntPrePreparationMethod": value}, "test")
        assert get_preparation_process(record) == clean_value


class TestIsOnLoan:
    def test_not_on_loan(self):
        record = SourceRecord("t1", {"some": "data"}, "test")
        assert not is_on_loan(record)

    def test_on_loan_irn(self):
        record = SourceRecord("t1", {"LocPermanentLocationRef": "3250522"}, "test")
        assert is_on_loan(record)

    def test_not_on_loan_summary_empty(self):
        record = SourceRecord("t1", {"LocCurrentSummaryData": ""}, "test")
        assert not is_on_loan(record)

    def test_not_on_loan_summary_a_place_in_the_nhm(self):
        location = "3G (West); B; 048; EGB.5.28; 5; Earth Galleries; South Kensington"
        record = SourceRecord("t1", {"LocCurrentSummaryData": location}, "test")
        assert not is_on_loan(record)

    def test_on_loan_summary_on_loan(self):
        record = SourceRecord("t1", {"LocCurrentSummaryData": "ON LOAN"}, "test")
        assert is_on_loan(record)
        record = SourceRecord("t1", {"LocCurrentSummaryData": "on loan"}, "test")
        assert is_on_loan(record)
        record = SourceRecord("t1", {"LocCurrentSummaryData": "On Loan"}, "test")
        assert is_on_loan(record)

    def test_on_loan_summary_exhibition_loan(self):
        record = SourceRecord(
            "t1", {"LocCurrentSummaryData": "On Exhibition Loan (see Events)"}, "test"
        )
        assert is_on_loan(record)

    def test_on_loan_summary_loose_loan(self):
        record = SourceRecord(
            "t1", {"LocCurrentSummaryData": "this is on loan somewhere"}, "test"
        )
        assert is_on_loan(record)
