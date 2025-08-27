from typing import List, Tuple

import pytest

from dataimporter.emu.views.preparation import (
    INVALID_PROJECT,
    INVALID_SUB_DEPARTMENT,
    ON_LOAN,
    PreparationView,
    get_preparation_process,
    is_on_loan,
)
from dataimporter.emu.views.specimen import SpecimenView
from dataimporter.emu.views.utils import (
    INVALID_DEPARTMENT,
    INVALID_GUID,
    INVALID_STATUS,
    INVALID_TYPE,
    NO_PUBLISH,
)
from dataimporter.lib.model import SourceRecord
from dataimporter.lib.view import SUCCESS_RESULT, FilterResult
from tests.helpers.samples.preparation import (
    SAMPLE_MAMMAL_PREPARATION_DATA,
    SAMPLE_MAMMAL_PREPARATION_ID,
    SAMPLE_PREPARATION_DATA,
    SAMPLE_PREPARATION_ID,
)
from tests.helpers.samples.specimen import SAMPLE_SPECIMEN_DATA, SAMPLE_SPECIMEN_ID
from tests.helpers.utils import is_member_error, is_publishable_error

mol_prep_is_publishable_member_scenarios: List[
    Tuple[dict, FilterResult, FilterResult, FilterResult]
] = [
    ({'ColRecordType': 'Specimen'}, *is_member_error(INVALID_TYPE)),
    # this is a check to make sure a mammal part in molecular collections doesn't come
    # through
    ({'ColRecordType': 'Mammal Group Part'}, *is_member_error(INVALID_SUB_DEPARTMENT)),
    ({'AdmPublishWebNoPasswordFlag': 'n'}, *is_publishable_error(NO_PUBLISH)),
    (
        {'AdmGUIDPreferredValue': 'not a valid guid!'},
        *is_publishable_error(INVALID_GUID),
    ),
    ({'SecRecordStatus': 'INVALID'}, *is_publishable_error(INVALID_STATUS)),
    ({'ColDepartment': 'DDI'}, *is_member_error(INVALID_DEPARTMENT)),
    ({'ColSubDepartment': 'Informatics'}, *is_member_error(INVALID_SUB_DEPARTMENT)),
    ({'ColSubDepartment': 'LS Mammals'}, *is_member_error(INVALID_SUB_DEPARTMENT)),
    ({'LocPermanentLocationRef': '3250522'}, *is_publishable_error(ON_LOAN)),
    ({'LocCurrentSummaryData': 'ON LOAN'}, *is_publishable_error(ON_LOAN)),
    ({}, SUCCESS_RESULT, SUCCESS_RESULT, SUCCESS_RESULT),
]


@pytest.mark.parametrize(
    'overrides, member_result, publishable_result, publishable_member_result',
    mol_prep_is_publishable_member_scenarios,
)
def test_is_publishable_member_mol_prep(
    overrides: dict,
    member_result: FilterResult,
    publishable_result: FilterResult,
    publishable_member_result: FilterResult,
    preparation_view: PreparationView,
):
    data = {**SAMPLE_PREPARATION_DATA, **overrides}
    record = SourceRecord(SAMPLE_PREPARATION_ID, data, 'test')
    assert preparation_view.is_member(record) == member_result
    assert preparation_view.is_publishable(record) == publishable_result
    assert preparation_view.is_publishable_member(record) == publishable_member_result


mammal_part_prep_is_publishable_member_scenarios: List[
    Tuple[dict, FilterResult, FilterResult, FilterResult]
] = [
    ({'ColRecordType': 'Specimen'}, *is_member_error(INVALID_TYPE)),
    ({'AdmPublishWebNoPasswordFlag': 'n'}, *is_publishable_error(NO_PUBLISH)),
    (
        {'AdmGUIDPreferredValue': 'not a valid guid!'},
        *is_publishable_error(INVALID_GUID),
    ),
    ({'SecRecordStatus': 'INVALID'}, *is_publishable_error(INVALID_STATUS)),
    ({'ColDepartment': 'DDI'}, *is_member_error(INVALID_DEPARTMENT)),
    ({'ColSubDepartment': 'Informatics'}, *is_member_error(INVALID_SUB_DEPARTMENT)),
    (
        {'ColSubDepartment': 'Molecular Collections'},
        *is_member_error(INVALID_SUB_DEPARTMENT),
    ),
    ({'NhmSecProjectName': 'Life of Darwin Tree'}, *is_member_error(INVALID_PROJECT)),
    # this is a check to make sure a prep in LS Mammals doesn't come through
    ({'ColRecordType': 'Preparation'}, *is_member_error(INVALID_SUB_DEPARTMENT)),
    ({'LocPermanentLocationRef': '3250522'}, *is_publishable_error(ON_LOAN)),
    ({'LocCurrentSummaryData': 'ON LOAN'}, *is_publishable_error(ON_LOAN)),
    ({}, SUCCESS_RESULT, SUCCESS_RESULT, SUCCESS_RESULT),
]


@pytest.mark.parametrize(
    'overrides, member_result, publishable_result, publishable_member_result',
    mammal_part_prep_is_publishable_member_scenarios,
)
def test_is_publishable_member_mammal_part_prep(
    overrides: dict,
    member_result: FilterResult,
    publishable_result: FilterResult,
    publishable_member_result: FilterResult,
    preparation_view: PreparationView,
):
    data = {**SAMPLE_MAMMAL_PREPARATION_DATA, **overrides}
    record = SourceRecord(SAMPLE_MAMMAL_PREPARATION_ID, data, 'test')
    assert preparation_view.is_member(record) == member_result
    assert preparation_view.is_publishable(record) == publishable_result
    assert preparation_view.is_publishable_member(record) == publishable_member_result


def test_transform_mol_prep(preparation_view: PreparationView):
    record = SourceRecord(SAMPLE_PREPARATION_ID, SAMPLE_PREPARATION_DATA, 'test')

    data = preparation_view.transform(record)
    assert data == {
        '_id': record.id,
        'created': '2022-09-12T17:07:51+00:00',
        'modified': '2022-09-12T17:21:14+00:00',
        'project': 'Darwin Tree of Life',
        'identifier': 'C9K02TWP_B2',
        'preparationType': 'DNA Extract',
        'preparationContents': '**OTHER_SOMATIC_ANIMAL_TISSUE**',
        'preparationDate': '2022-05-09',
        'occurrenceID': 'f11c9c35-4da5-45e5-9dbb-6f8f55b26aa7',
        'purpose': 'DNA barcoding only',
    }


def test_transform_mammal_part(preparation_view: PreparationView):
    record = SourceRecord(
        SAMPLE_MAMMAL_PREPARATION_ID, SAMPLE_MAMMAL_PREPARATION_DATA, 'test'
    )

    data = preparation_view.transform(record)
    assert data == {
        '_id': record.id,
        'created': '2023-05-02T14:55:51+00:00',
        'modified': '2023-05-02T14:55:51+00:00',
        'project': 'Darwin Tree of Life',
        'identifier': 'FF06063966',
        'preparationProcess': 'Flash Freezing: Dry Ice',
        'preparationContents': 'MUSCLE',
        'occurrenceID': '541cb421-2a3f-4699-ad3b-8030f36afffa',
        'purpose': 'DNA barcoding only',
    }


def test_transform_mol_prep_with_voucher_direct(
    preparation_view: PreparationView, specimen_view: SpecimenView
):
    record = SourceRecord(SAMPLE_PREPARATION_ID, SAMPLE_PREPARATION_DATA, 'test')

    # add a specimen record to the specimen view's store
    specimen_record = SourceRecord(SAMPLE_SPECIMEN_ID, SAMPLE_SPECIMEN_DATA, 'test')
    specimen_view.store.put([specimen_record])
    specimen_occurrence_id = SAMPLE_SPECIMEN_DATA['AdmGUIDPreferredValue']

    data = preparation_view.transform(record)
    assert data == {
        '_id': record.id,
        'created': '2022-09-12T17:07:51+00:00',
        'modified': '2022-09-12T17:21:14+00:00',
        'project': 'Darwin Tree of Life',
        'identifier': 'C9K02TWP_B2',
        'preparationType': 'DNA Extract',
        'preparationContents': '**OTHER_SOMATIC_ANIMAL_TISSUE**',
        'preparationDate': '2022-05-09',
        'associatedOccurrences': f'Voucher: {specimen_occurrence_id}',
        'scientificName': 'Synodontis schall (Bloch & Schneider, 1801)',
        'order': 'Siluriformes',
        'barcode': '013234322',
        'decimalLatitude': '10.0833333',
        'decimalLongitude': '35.6333333',
        'locality': 'Forward base three, Mouth of Didessa River, Blue Nile Gorge, Ethiopia, Alt. 900 m',
        'occurrenceID': 'f11c9c35-4da5-45e5-9dbb-6f8f55b26aa7',
        'purpose': 'DNA barcoding only',
    }


def test_transform_mol_prep_with_voucher_indirect(
    preparation_view: PreparationView, specimen_view: SpecimenView
):
    # replace the parent ref ID with parentPrep
    prep_data = SAMPLE_PREPARATION_DATA.copy()
    prep_data['EntPreSpecimenRef'] = 'parentPrep'
    record = SourceRecord(SAMPLE_PREPARATION_ID, prep_data, 'test')

    # add another prep as the parent with parentPrep as the ID
    parent_prep_record = SourceRecord('parentPrep', SAMPLE_PREPARATION_DATA, 'test')
    preparation_view.store.put([parent_prep_record])

    # add a specimen record to the specimen view's store
    specimen_record = SourceRecord(SAMPLE_SPECIMEN_ID, SAMPLE_SPECIMEN_DATA, 'test')
    specimen_view.store.put([specimen_record])
    specimen_occurrence_id = SAMPLE_SPECIMEN_DATA['AdmGUIDPreferredValue']

    data = preparation_view.transform(record)
    assert data == {
        '_id': record.id,
        'created': '2022-09-12T17:07:51+00:00',
        'modified': '2022-09-12T17:21:14+00:00',
        'project': 'Darwin Tree of Life',
        'identifier': 'C9K02TWP_B2',
        'preparationType': 'DNA Extract',
        'preparationContents': '**OTHER_SOMATIC_ANIMAL_TISSUE**',
        'preparationDate': '2022-05-09',
        'associatedOccurrences': f'Voucher: {specimen_occurrence_id}',
        'scientificName': 'Synodontis schall (Bloch & Schneider, 1801)',
        'order': 'Siluriformes',
        'barcode': '013234322',
        'decimalLatitude': '10.0833333',
        'decimalLongitude': '35.6333333',
        'locality': 'Forward base three, Mouth of Didessa River, Blue Nile Gorge, Ethiopia, Alt. 900 m',
        'occurrenceID': 'f11c9c35-4da5-45e5-9dbb-6f8f55b26aa7',
        'purpose': 'DNA barcoding only',
    }


def test_transform_mammal_part_with_voucher_direct(
    preparation_view: PreparationView, specimen_view: SpecimenView
):
    record = SourceRecord(
        SAMPLE_MAMMAL_PREPARATION_ID, SAMPLE_MAMMAL_PREPARATION_DATA, 'test'
    )

    # add a specimen record to the specimen view's store
    specimen_record = SourceRecord(SAMPLE_SPECIMEN_ID, SAMPLE_SPECIMEN_DATA, 'test')
    specimen_view.store.put([specimen_record])
    specimen_occurrence_id = SAMPLE_SPECIMEN_DATA['AdmGUIDPreferredValue']

    data = preparation_view.transform(record)
    assert data == {
        '_id': record.id,
        'created': '2023-05-02T14:55:51+00:00',
        'modified': '2023-05-02T14:55:51+00:00',
        'project': 'Darwin Tree of Life',
        'identifier': 'FF06063966',
        'preparationProcess': 'Flash Freezing: Dry Ice',
        'preparationContents': 'MUSCLE',
        'associatedOccurrences': f'Voucher: {specimen_occurrence_id}',
        'scientificName': 'Synodontis schall (Bloch & Schneider, 1801)',
        'order': 'Siluriformes',
        'barcode': '013234322',
        'decimalLatitude': '10.0833333',
        'decimalLongitude': '35.6333333',
        'locality': 'Forward base three, Mouth of Didessa River, Blue Nile Gorge, Ethiopia, Alt. 900 m',
        'occurrenceID': '541cb421-2a3f-4699-ad3b-8030f36afffa',
        'purpose': 'DNA barcoding only',
    }


def test_transform_mammal_part_with_voucher_indirect(
    preparation_view: PreparationView, specimen_view: SpecimenView
):
    # replace the parent ref ID with parentPrep
    prep_data = SAMPLE_MAMMAL_PREPARATION_DATA.copy()
    prep_data['RegRegistrationParentRef'] = 'parentPrep'
    record = SourceRecord(SAMPLE_MAMMAL_PREPARATION_ID, prep_data, 'test')

    # add another prep as the parent with parentPrep as the ID
    parent_prep_record = SourceRecord('parentPrep', SAMPLE_PREPARATION_DATA, 'test')
    preparation_view.store.put([parent_prep_record])

    # add a specimen record to the specimen view's store
    specimen_record = SourceRecord(SAMPLE_SPECIMEN_ID, SAMPLE_SPECIMEN_DATA, 'test')
    specimen_view.store.put([specimen_record])
    specimen_occurrence_id = SAMPLE_SPECIMEN_DATA['AdmGUIDPreferredValue']

    data = preparation_view.transform(record)
    assert data == {
        '_id': record.id,
        'created': '2023-05-02T14:55:51+00:00',
        'modified': '2023-05-02T14:55:51+00:00',
        'project': 'Darwin Tree of Life',
        'identifier': 'FF06063966',
        'preparationProcess': 'Flash Freezing: Dry Ice',
        'preparationContents': 'MUSCLE',
        'associatedOccurrences': f'Voucher: {specimen_occurrence_id}',
        'scientificName': 'Synodontis schall (Bloch & Schneider, 1801)',
        'order': 'Siluriformes',
        'barcode': '013234322',
        'decimalLatitude': '10.0833333',
        'decimalLongitude': '35.6333333',
        'locality': 'Forward base three, Mouth of Didessa River, Blue Nile Gorge, Ethiopia, Alt. 900 m',
        'occurrenceID': '541cb421-2a3f-4699-ad3b-8030f36afffa',
        'purpose': 'DNA barcoding only',
    }


process_cleaning_scenarios = [
    # these two shouldn't happen but might as well check
    (None, None),
    ('', None),
    ('100% ethanol', '100% ethanol'),
    # lowercase, all variants
    ('killing agent: 100% ethanol', '100% ethanol'),
    ('killing agent:     100% ethanol', '100% ethanol'),
    ('killing agent 100% ethanol', '100% ethanol'),
    ('killing agent     100% ethanol', '100% ethanol'),
    # mixed case, all variants
    ('kilLing AgenT: 100% ethanol', '100% ethanol'),
    ('kilLing AgenT:      100% ethanol', '100% ethanol'),
    ('kilLing AgenT 100% ethanol', '100% ethanol'),
    ('kilLing AgenT       100% ethanol', '100% ethanol'),
    # killing agent mentioned more than once
    ('killing agent: plant killing agent', 'plant killing agent'),
]


class TestGetPreparationProcess:
    def test_missing(self):
        record = SourceRecord('t1', {'oh': 'no!'}, 'test')
        assert get_preparation_process(record) is None

    @pytest.mark.parametrize('value,clean_value', process_cleaning_scenarios)
    def test_clean(self, value, clean_value):
        record = SourceRecord('t1', {'EntPrePreparationMethod': value}, 'test')
        assert get_preparation_process(record) == clean_value


class TestIsOnLoan:
    def test_not_on_loan(self):
        record = SourceRecord('t1', {'some': 'data'}, 'test')
        assert not is_on_loan(record)

    def test_on_loan_irn(self):
        record = SourceRecord('t1', {'LocPermanentLocationRef': '3250522'}, 'test')
        assert is_on_loan(record)

    def test_not_on_loan_summary_empty(self):
        record = SourceRecord('t1', {'LocCurrentSummaryData': ''}, 'test')
        assert not is_on_loan(record)

    def test_not_on_loan_summary_a_place_in_the_nhm(self):
        location = '3G (West); B; 048; EGB.5.28; 5; Earth Galleries; South Kensington'
        record = SourceRecord('t1', {'LocCurrentSummaryData': location}, 'test')
        assert not is_on_loan(record)

    def test_on_loan_summary_on_loan(self):
        record = SourceRecord('t1', {'LocCurrentSummaryData': 'ON LOAN'}, 'test')
        assert is_on_loan(record)
        record = SourceRecord('t1', {'LocCurrentSummaryData': 'on loan'}, 'test')
        assert is_on_loan(record)
        record = SourceRecord('t1', {'LocCurrentSummaryData': 'On Loan'}, 'test')
        assert is_on_loan(record)

    def test_on_loan_summary_exhibition_loan(self):
        record = SourceRecord(
            't1', {'LocCurrentSummaryData': 'On Exhibition Loan (see Events)'}, 'test'
        )
        assert is_on_loan(record)

    def test_on_loan_summary_loose_loan(self):
        record = SourceRecord(
            't1', {'LocCurrentSummaryData': 'this is on loan somewhere'}, 'test'
        )
        assert is_on_loan(record)


class TestGetVoucherData:
    def test_no_voucher(self, preparation_view: PreparationView):
        record = SourceRecord('1', {'EntPreSpecimenRef': '2'}, 'test')
        assert preparation_view.get_voucher_data(record) is None

        record = SourceRecord('1', {'RegRegistrationParentRef': '2'}, 'test')
        assert preparation_view.get_voucher_data(record) is None

    def test_prep_specimen_voucher(
        self, preparation_view: PreparationView, specimen_view: SpecimenView
    ):
        prep = SourceRecord('1', {'EntPreSpecimenRef': '2'}, 'test')
        specimen = SourceRecord('2', SAMPLE_SPECIMEN_DATA, 'test')

        preparation_view.store.put([prep, specimen])

        assert preparation_view.get_voucher_data(prep) == specimen_view.transform(
            specimen
        )

    def test_prep_specimen_voucher_but_invalid_specimen(
        self, preparation_view: PreparationView
    ):
        prep = SourceRecord('1', {'EntPreSpecimenRef': '2'}, 'test')
        # this is an invalid specimen (specimen_view.is_member will fail)
        specimen = SourceRecord('2', {'type': 'specimen', 'lol': 'yeah'}, 'test')

        preparation_view.store.put([prep, specimen])

        assert preparation_view.get_voucher_data(prep) is None

    def test_prep_multiple_parents_specimen_voucher_but_invalid_specimen(
        self, preparation_view: PreparationView, specimen_view: SpecimenView
    ):
        """
        This is not a scenario that is going to happen, but it proves the traversal of
        the links works under strenuous conditions.
        """
        prep = SourceRecord('1', {'EntPreSpecimenRef': '2'}, 'test')
        parents = [
            SourceRecord(str(i), {'EntPreSpecimenRef': str(i + 1)}, 'test')
            for i in range(2, 10)
        ]
        specimen = SourceRecord('9', SAMPLE_SPECIMEN_DATA, 'test')

        preparation_view.store.put([prep, *parents, specimen])

        assert preparation_view.get_voucher_data(prep) == specimen_view.transform(
            specimen
        )

    def test_mammal_part_specimen_voucher(
        self, preparation_view: PreparationView, specimen_view: SpecimenView
    ):
        prep = SourceRecord(
            '1',
            {'ColRecordType': 'Mammal Group Part', 'RegRegistrationParentRef': '2'},
            'test',
        )
        specimen = SourceRecord('2', SAMPLE_SPECIMEN_DATA, 'test')

        preparation_view.store.put([prep, specimen])

        assert preparation_view.get_voucher_data(prep) == specimen_view.transform(
            specimen
        )

    def test_self_ref_loop(self, preparation_view: PreparationView):
        prep = SourceRecord('1', {'EntPreSpecimenRef': '1'}, 'test')
        preparation_view.store.put([prep])
        assert preparation_view.get_voucher_data(prep) is None

    def test_longer_loop(self, preparation_view: PreparationView):
        prep_1 = SourceRecord('1', {'EntPreSpecimenRef': '2'}, 'test')
        prep_2 = SourceRecord(
            '2',
            {'ColRecordType': 'Mammal Group Part', 'RegRegistrationParentRef': '3'},
            'test',
        )
        prep_3 = SourceRecord('3', {'EntPreSpecimenRef': '4'}, 'test')
        prep_4 = SourceRecord('4', {'EntPreSpecimenRef': '2'}, 'test')
        preparation_view.store.put([prep_1, prep_2, prep_3, prep_4])
        assert preparation_view.get_voucher_data(prep_1) is None
