from pathlib import Path
from typing import List, Tuple

import pytest

from dataimporter.dbs import DataDB
from dataimporter.emu.views.specimen import SpecimenView
from dataimporter.emu.views.utils import (
    INVALID_TYPE,
    NO_PUBLISH,
    INVALID_GUID,
    INVALID_STATUS,
    INVALID_DEPARTMENT,
)
from dataimporter.model import SourceRecord
from dataimporter.view import FilterResult, SUCCESS_RESULT
from tests.helpers.samples.specimen import SAMPLE_SPECIMEN_DATA, SAMPLE_SPECIMEN_ID


@pytest.fixture
def specimen_view(tmp_path: Path) -> SpecimenView:
    view = SpecimenView(tmp_path / "specimen_view", DataDB(tmp_path / "specimen_data"))
    yield view
    view.close()


is_member_scenarios: List[Tuple[dict, FilterResult]] = [
    ({"ColRecordType": "Artefact"}, INVALID_TYPE),
    ({"AdmPublishWebNoPasswordFlag": "n"}, NO_PUBLISH),
    ({"AdmGUIDPreferredValue": "not a valid guid!"}, INVALID_GUID),
    ({"SecRecordStatus": "INVALID"}, INVALID_STATUS),
    ({"ColDepartment": "DDI"}, INVALID_DEPARTMENT),
    ({}, SUCCESS_RESULT),
]


@pytest.mark.parametrize("overrides, result", is_member_scenarios)
def test_is_member(overrides: dict, result: FilterResult, specimen_view: SpecimenView):
    data = {**SAMPLE_SPECIMEN_DATA, **overrides}
    record = SourceRecord(SAMPLE_SPECIMEN_ID, data, "test")
    assert specimen_view.is_member(record) == result


def test_transform_deleted(specimen_view: SpecimenView):
    record = SourceRecord(SAMPLE_SPECIMEN_ID, {}, "test")
    assert record.is_deleted

    data = specimen_view.transform(record)
    assert data == {}


def test_make_data(specimen_view: SpecimenView):
    record = SourceRecord(SAMPLE_SPECIMEN_ID, SAMPLE_SPECIMEN_DATA, "test")

    data = specimen_view.make_data(record)
    assert data == {
        "_id": "2531732",
        "age": None,
        "ageType": None,
        "barcode": "013234322",
        "basisOfRecord": "PreservedSpecimen",
        "bed": None,
        "catalogNumber": "1968.11.11.17-18",
        "catalogueDescription": None,
        "chondriteAchondrite": None,
        "chronostratigraphy": None,
        "class": "Actinopterygii",
        "clutchSize": None,
        "collectionCode": "ZOO",
        "collectionKind": None,
        "collectionName": None,
        "commodity": None,
        "continent": "Africa",
        "coordinateUncertaintyInMeters": None,
        "country": "Ethiopia",
        "created": "2003-06-20T09:24:24+00:00",
        "dateIdentified": None,
        "dateRegistered": None,
        "day": "16",
        "decimalLatitude": "10.0833333",
        "decimalLongitude": "35.6333333",
        "depositType": None,
        "determinationFiledAs": "Yes",
        "determinationNames": "Synodontis schall (Bloch & Schneider, 1801)",
        "determinationTypes": None,
        "donorName": "Sandhurst Ethiopian Expedition 1968",
        "earliestAgeOrLowestStage": None,
        "earliestEonOrLowestEonothem": None,
        "earliestEpochOrLowestSeries": None,
        "earliestEraOrLowestErathem": None,
        "earliestPeriodOrLowestSystem": None,
        "eventTime": None,
        "expedition": "Sandhurst Ethiopian Expedition 1968",
        "exsiccata": None,
        "exsiccataNumber": None,
        "extractionMethod": None,
        "family": "Mochokidae",
        "fieldNumber": None,
        "formation": None,
        "genus": "Synodontis",
        "geodeticDatum": None,
        "geologyRegion": None,
        "georeferenceProtocol": None,
        "group": None,
        "habitat": None,
        "higherClassification": "Actinopterygii; Siluriformes; Mochokidae",
        "higherGeography": "Africa; Ethiopia; Benshangul-Gumaz; Metekel",
        "highestBiostratigraphicZone": None,
        "hostRock": None,
        "identificationAsRegistered": None,
        "identificationDescription": None,
        "identificationOther": None,
        "identificationQualifier": None,
        "identificationVariety": None,
        "identifiedBy": None,
        "individualCount": "2",
        "infraspecificEpithet": None,
        "institutionCode": "NHMUK",
        "island": None,
        "islandGroup": None,
        "kindOfCollection": None,
        "kindOfObject": "Spirit",
        "kingdom": None,
        "labelLocality": None,
        "latestAgeOrHighestStage": None,
        "latestEonOrHighestEonothem": None,
        "latestEpochOrHighestSeries": None,
        "latestEraOrHighestErathem": None,
        "latestPeriodOrHighestSystem": None,
        "lifeStage": None,
        "lithostratigraphy": None,
        "locality": "Forward base three, Mouth of Didessa River, Blue Nile Gorge, "
        "Ethiopia, Alt. 900 m",
        "lowestBiostratigraphicZone": None,
        "maximumDepthInMeters": None,
        "maximumElevationInMeters": None,
        "member": None,
        "meteoriteClass": None,
        "meteoriteGroup": None,
        "meteoriteType": None,
        "mine": None,
        "mineralComplex": None,
        "minimumDepthInMeters": None,
        "minimumElevationInMeters": None,
        "miningDistrict": None,
        "modified": "2023-10-18T19:11:01+00:00",
        "month": "9",
        "nestShape": None,
        "nestSite": None,
        "observedWeight": None,
        "occurrence": None,
        "occurrenceID": "4ffbec35-4397-440b-b781-a18b4d958cef",
        "occurrenceStatus": "present",
        "order": "Siluriformes",
        "otherCatalogNumbers": "NHMUK:ecatalogue:2531732",
        "partType": None,
        "petrologySubtype": None,
        "petrologyType": None,
        "phylum": None,
        "plantDescription": None,
        "populationCode": None,
        "preparationType": None,
        "preparations": None,
        "preservative": "IMS 70%",
        "project": None,
        "recordNumber": None,
        "recordedBy": None,
        "recovery": None,
        "recoveryDate": None,
        "recoveryWeight": None,
        "registeredWeight": None,
        "registeredWeightUnit": None,
        "registrationCode": "PI03",
        "resuspendedIn": None,
        "samplingProtocol": None,
        "scientificName": "Synodontis schall (Bloch & Schneider, 1801)",
        "scientificNameAuthorship": "Bloch & Schneider, 1801",
        "setMark": None,
        "sex": None,
        "specificEpithet": "schall",
        "stateProvince": "Benshangul-Gumaz",
        "subDepartment": "LS Fish",
        "subgenus": None,
        "taxonRank": None,
        "tectonicProvince": None,
        "texture": None,
        "totalVolume": None,
        "typeStatus": None,
        "verbatimLatitude": "10 05 00.000 N",
        "verbatimLongitude": "035 38 00.000 E",
        "vessel": None,
        "viceCounty": None,
        "waterBody": "Blue Nile",
        "year": "1968",
    }
