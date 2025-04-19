from typing import List, Optional, Tuple

import pytest

from dataimporter.emu.views.image import ImageView
from dataimporter.emu.views.mammalpart import MammalPartView
from dataimporter.emu.views.specimen import (
    SpecimenView,
    clean_determination_names,
    get_first_non_person_string,
    get_individual_count,
    person_string_remover,
)
from dataimporter.emu.views.taxonomy import TaxonomyView
from dataimporter.emu.views.utils import (
    INVALID_DEPARTMENT,
    INVALID_GUID,
    INVALID_STATUS,
    INVALID_TYPE,
    NO_PUBLISH,
)
from dataimporter.ext.gbif import GBIFView
from dataimporter.lib.model import SourceRecord
from dataimporter.lib.view import SUCCESS_RESULT, FilterResult
from tests.helpers.samples.image import SAMPLE_IMAGE_DATA, SAMPLE_IMAGE_ID
from tests.helpers.samples.mammalpart import (
    SAMPLE_MAMMAL_PART_DATA,
    SAMPLE_MAMMAL_PART_ID,
)
from tests.helpers.samples.specimen import SAMPLE_SPECIMEN_DATA, SAMPLE_SPECIMEN_ID
from tests.helpers.samples.taxonomy import SAMPLE_TAXONOMY_DATA, SAMPLE_TAXONOMY_ID


@pytest.mark.parametrize(
    "count, expected", [("0", None), ("1", "1"), ("-1", None), ("", None), (None, None)]
)
def test_get_individual_count(count: str, expected: Optional[str]):
    if count:
        data = {"DarIndividualCount": count}
    else:
        data = {"not the count, this record has no count": "beans"}

    record = SourceRecord("1", data, "test")
    assert get_individual_count(record) == expected


person_string_scenarios = [
    ("Person String", None),
    ("Person String     ", None),
    ("banana", "banana"),
    (
        "Person String; Bert Lemon; Greg Person; Gwen String;",
        "Bert Lemon; Greg Person; Gwen String;",
    ),
    (
        "Person String Lyall McCheese Molly Sack Person String",
        "Lyall McCheese Molly Sack",
    ),
    (
        "Person String;Bert Lemon;Greg Person;Gwen String;",
        "Bert Lemon;Greg Person;Gwen String;",
    ),
    (
        "<name>, Person String; <name>, Person String; <name>, Person String; <name>",
        "<name>, <name>, <name>, <name>",
    ),
]


@pytest.mark.parametrize("value, expected", person_string_scenarios)
def test_person_string_remover(value: str, expected: Optional[str]):
    assert person_string_remover(value) == expected


def test_get_first_non_person_string():
    assert get_first_non_person_string([]) is None
    assert get_first_non_person_string(["banana"]) == "banana"
    assert get_first_non_person_string(["Person String", "banana"]) == "banana"


def test_clean_determination_names():
    assert clean_determination_names([]) is None
    assert clean_determination_names(["banana"]) == ("banana",)
    assert clean_determination_names(["Person String", "banana"]) == (None, "banana")
    assert clean_determination_names(
        ["Person String", "banana", "Person String; lemon"]
    ) == (None, "banana", "lemon")


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


def test_transform_no_linked_data(specimen_view: SpecimenView):
    record = SourceRecord(SAMPLE_SPECIMEN_ID, SAMPLE_SPECIMEN_DATA, "test")

    data = specimen_view.transform(record)
    assert data == {
        "_id": "2531732",
        "barcode": "013234322",
        "basisOfRecord": "PreservedSpecimen",
        "catalogNumber": "1968.11.11.17-18",
        "class": "Actinopterygii",
        "collectionCode": "ZOO",
        "continent": "Africa",
        "country": "Ethiopia",
        "created": "2003-06-20T09:24:24+00:00",
        "day": "16",
        "decimalLatitude": "10.0833333",
        "decimalLongitude": "35.6333333",
        "determinationFiledAs": ("Yes",),
        "determinationNames": ("Synodontis schall (Bloch & Schneider, 1801)",),
        "donorName": "Sandhurst Ethiopian Expedition 1968",
        "expedition": "Sandhurst Ethiopian Expedition 1968",
        "family": "Mochokidae",
        "genus": "Synodontis",
        "higherClassification": "Actinopterygii; Siluriformes; Mochokidae",
        "higherGeography": "Africa; Ethiopia; Benshangul-Gumaz; Metekel",
        "individualCount": "2",
        "institutionCode": "NHMUK",
        "kindOfObject": "Spirit",
        "locality": "Forward base three, Mouth of Didessa River, Blue Nile Gorge, "
        "Ethiopia, Alt. 900 m",
        "modified": "2023-10-18T19:11:01+00:00",
        "month": "9",
        "occurrenceID": "4ffbec35-4397-440b-b781-a18b4d958cef",
        "occurrenceStatus": "present",
        "order": "Siluriformes",
        "otherCatalogNumbers": "NHMUK:ecatalogue:2531732",
        "preservative": "IMS 70%",
        "registrationCode": "PI03",
        "scientificName": "Synodontis schall (Bloch & Schneider, 1801)",
        "scientificNameAuthorship": "Bloch & Schneider, 1801",
        "specificEpithet": "schall",
        "stateProvince": "Benshangul-Gumaz",
        "subDepartment": "LS Fish",
        "verbatimLatitude": "10 05 00.000 N",
        "verbatimLongitude": "035 38 00.000 E",
        "waterBody": "Blue Nile",
        "year": "1968",
    }


# skipping because we have removed the mammal part/parent link data
@pytest.mark.skip
def test_transform_with_linked_data(
    specimen_view: SpecimenView,
    image_view: ImageView,
    taxonomy_view: TaxonomyView,
    mammal_part_view: MammalPartView,
    gbif_view: GBIFView,
):
    record = SourceRecord(SAMPLE_SPECIMEN_ID, SAMPLE_SPECIMEN_DATA, "test")

    # add an image to the image view's store
    image_record = SourceRecord(SAMPLE_IMAGE_ID, SAMPLE_IMAGE_DATA, "test")
    image_view.store.put([image_record])

    # add a taxonomy record to the taxonomy view's store
    taxonomy_record = SourceRecord(SAMPLE_TAXONOMY_ID, SAMPLE_TAXONOMY_DATA, "test")
    taxonomy_view.store.put([taxonomy_record])

    # add mammal part record to the mammal parts view's store
    mp_record = SourceRecord(SAMPLE_MAMMAL_PART_ID, SAMPLE_MAMMAL_PART_DATA, "test")
    mammal_part_view.store.put([mp_record])

    # add gbif record to the gbif view's store
    issues = (
        "GEODETIC_DATUM_ASSUMED_WGS84",
        "INSTITUTION_MATCH_FUZZY",
        "COLLECTION_MATCH_NONE",
        "MULTIMEDIA_URI_INVALID",
    )
    gbif_record = SourceRecord(
        "100",
        {
            "issue": ";".join(issues),
            "occurrenceID": SAMPLE_SPECIMEN_DATA["AdmGUIDPreferredValue"],
        },
        "test",
    )
    gbif_view.store.put([gbif_record])

    data = specimen_view.transform(record)
    assert data == {
        "_id": record.id,
        "barcode": "013234322",
        "basisOfRecord": "PreservedSpecimen",
        "catalogNumber": "1968.11.11.17-18",
        "class": "Actinopterygii",
        "collectionCode": "ZOO",
        "continent": "Africa",
        "country": "Ethiopia",
        "created": "2003-06-20T09:24:24+00:00",
        "day": "16",
        "decimalLatitude": "10.0833333",
        "decimalLongitude": "35.6333333",
        "determinationFiledAs": ("Yes",),
        "determinationNames": ("Synodontis schall (Bloch & Schneider, 1801)",),
        "donorName": "Sandhurst Ethiopian Expedition 1968",
        "expedition": "Sandhurst Ethiopian Expedition 1968",
        "family": "Mochokidae",
        "genus": "Synodontis",
        "higherClassification": "Actinopterygii; Siluriformes; Mochokidae",
        "higherGeography": "Africa; Ethiopia; Benshangul-Gumaz; Metekel",
        "individualCount": "2",
        "institutionCode": "NHMUK",
        "kindOfObject": "Spirit",
        "locality": "Forward base three, Mouth of Didessa River, Blue Nile Gorge, "
        "Ethiopia, Alt. 900 m",
        "modified": "2023-10-18T19:11:01+00:00",
        "month": "9",
        "occurrenceID": "4ffbec35-4397-440b-b781-a18b4d958cef",
        "occurrenceStatus": "present",
        "order": "Siluriformes",
        "otherCatalogNumbers": "NHMUK:ecatalogue:2531732",
        "preservative": "IMS 70%",
        "registrationCode": "PI03",
        "scientificName": "Synodontis schall (Bloch & Schneider, 1801)",
        "scientificNameAuthorship": "Bloch & Schneider, 1801",
        "specificEpithet": "schall",
        "stateProvince": "Benshangul-Gumaz",
        "subDepartment": "LS Fish",
        "verbatimLatitude": "10 05 00.000 N",
        "verbatimLongitude": "035 38 00.000 E",
        "waterBody": "Blue Nile",
        "year": "1968",
        # added via the image link
        "associatedMedia": [image_view.transform(image_record)],
        "associatedMediaCount": 1,
        # added via the taxonomy link
        "currentScientificName": "Microterys colligatus (Walker, 1872)",
        "phylum": "Arthropoda",
        "subfamily": "Encyrtinae",
        "suborder": "Parasitica",
        "superfamily": "Chalcidoidea",
        "taxonRank": "Species",
        # added via the mammal part link
        "preparations": "Skull",
        # added via the gbif link
        "gbifID": "100",
        "gbifIssue": issues,
    }
