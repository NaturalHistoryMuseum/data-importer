from typing import Optional, Iterable, Tuple

from fastnumbers import try_int

from dataimporter.emu.views.utils import (
    NO_PUBLISH,
    DISALLOWED_STATUSES,
    DEPARTMENT_COLLECTION_CODES,
    INVALID_STATUS,
    INVALID_DEPARTMENT,
    INVALID_TYPE,
    is_web_published,
    is_valid_guid,
    INVALID_GUID,
    translate_collection_code,
)
from dataimporter.emu.views.utils import emu_date
from dataimporter.lib.model import SourceRecord
from dataimporter.lib.view import View, FilterResult, SUCCESS_RESULT

ALLOWED_TYPES = {
    "specimen",
    "Specimen",
    "Specimen - single",
    "Specimen - multiple",
    "DNA Prep",
    "Mammal Group Parent",
    "Parasite Card",
    "Silica Gel Specimen",
    "Bryozoa Gel Specimen",
    "Parasite card",
}

BASIS_OF_RECORD_LOOKUP = {
    "Botany": "PreservedSpecimen",
    "Entomology": "PreservedSpecimen",
    "Zoology": "PreservedSpecimen",
    "Paleontology": "FossilSpecimen",
    "Mineralogy": "Occurrence",
}


def get_individual_count(record: SourceRecord) -> Optional[str]:
    """
    Returns the individual count value from the record only if it is available, and it
    is greater than 0. We need to do this because we have a lot of records where
    DarIndividualCount = "0" and by passing this on to the Portal we confuse users and
    cause problems on GBIF too as they interpret this to mean an absence of a specimen.

    :return: the individual count value or None
    """
    value = record.get_first_value("DarIndividualCount", default="")
    count = try_int(value, on_fail=None)
    if count is not None and count > 0:
        return value
    return None


def person_string_remover(value: str) -> Optional[str]:
    """
    Given a value, remove any occurrences of "Person String" in it. If after removing
    all "Person String" occurrences from the value there is no string left (after
    stripping!) return None.

    I can't remember the EMu side reasons for this, but for a few fields which have
    people's names in, we get "Person String" inserted into values, and we need to
    remove it.

    :param value: the field value
    :return: a cleaned version with no "Person String"s, None, or just the unchanged
             value
    """
    if "Person String" in value:
        # straight match to start with
        if "Person String" == value:
            return None
        # - with a space, catches this kind of thing:
        #   <name>, Person String; <name>, Person String; <name>, Person String; <name>
        #   and turns it into:
        #   <name>, <name>, <name>, <name>
        # - without a space just catches oddities where there is no space
        # - without a semicolon ensures that even if we don't do it cleanly, we remove
        #   the Person String
        for search in ("Person String; ", "Person String;", "Person String"):
            if search in value:
                new_search = value.replace(search, "").strip()
                return new_search if new_search else None
    return value


def get_first_non_person_string(values: Iterable[str]) -> Optional[str]:
    """
    Retrieve the first value from the values iterable which is non-None after being
    passed through the person string remover.

    :param values: the values to filter
    :return: None if no values are valid, otherwise the first valid string
    """
    return next(filter(None, map(person_string_remover, values)), None)


def get_all_non_person_strings(values: Iterable[str]) -> Optional[Tuple[str, ...]]:
    """
    Retrieve all values from the values iterable which are non-None after being passed
    through the person string remover.

    :param values: the values to filter
    :return: None if no values are valid, otherwise a tuple of valid strings
    """
    names = tuple(filter(None, map(person_string_remover, values)))
    return None if not names else names


def clean_determination_names(values: Iterable[str]) -> Optional[Tuple[str, ...]]:
    """
    Clean the determination names field values. This is a special function which passes
    the given values through the person string remover and then returns either None or a
    tuple. This does essentially the same as reduce=False parameter on the
    SourceRecord's get_all_values method but with the added person string removal.

    :param values: an iterable of values to clean
    :return: None if there are no values
    """
    filtered_values = tuple(map(person_string_remover, values))
    if len(filtered_values) == 0:
        return None
    return filtered_values


class SpecimenView(View):
    """
    View for specimen records.

    This view populates the specimen resource on the Data Portal.
    """

    def is_member(self, record: SourceRecord) -> FilterResult:
        """
        Filters the given record, determining whether it should be included in the
        specimens resource or not.

        :param record: the record to filter
        :return: a FilterResult object
        """
        if record.get_first_value("ColRecordType") not in ALLOWED_TYPES:
            return INVALID_TYPE

        if not is_web_published(record):
            return NO_PUBLISH

        if not is_valid_guid(record):
            return INVALID_GUID

        if record.get_first_value("SecRecordStatus") in DISALLOWED_STATUSES:
            return INVALID_STATUS

        if record.get_first_value("ColDepartment") not in DEPARTMENT_COLLECTION_CODES:
            return INVALID_DEPARTMENT

        return SUCCESS_RESULT

    def make_data(self, record: SourceRecord) -> dict:
        """
        Converts the record's raw data to a dict which will be the data presented on the
        Data Portal.

        :param record: the record to project
        :return: a dict containing the data for this record that should be displayed on
                 the Data Portal
        """
        # cache these for perf
        get_all = record.get_all_values
        get_first = record.get_first_value
        iter_all_values = record.iter_all_values

        return {
            # record level
            "_id": record.id,
            "modified": emu_date(
                get_first("AdmDateModified"), get_first("AdmTimeModified")
            ),
            "institutionCode": "NHMUK",
            "otherCatalogNumbers": f"NHMUK:ecatalogue:{record.id}",
            "basisOfRecord": BASIS_OF_RECORD_LOOKUP.get(get_first("ColDepartment")),
            "collectionCode": translate_collection_code(get_first("ColDepartment")),
            "occurrenceStatus": "present",
            # location
            "decimalLatitude": get_first("DarDecimalLatitude"),
            "decimalLongitude": get_first("DarDecimalLongitude"),
            "coordinateUncertaintyInMeters": get_first(
                "DarCoordinateUncertaintyInMeter"
            ),
            "verbatimLongitude": get_first("sumPreferredCentroidLongitude"),
            "verbatimLatitude": get_first("sumPreferredCentroidLatitude"),
            "locality": get_first("sumPreciseLocation"),
            "minimumDepthInMeters": get_first(
                "CollEventFromMetres", "DarMinimumDepthInMeters"
            ),
            "maximumDepthInMeters": get_first(
                "CollEventToMetres", "DarMaximumDepthInMeters"
            ),
            "country": get_first("DarCountry"),
            "waterBody": get_first("DarWaterBody"),
            "stateProvince": get_first("DarStateProvince"),
            "continent": get_first("DarContinent"),
            "island": get_first("DarIsland"),
            "islandGroup": get_first("DarIslandGroup"),
            "higherGeography": get_first("DarHigherGeography"),
            "geodeticDatum": get_first("DarGeodeticDatum"),
            "georeferenceProtocol": get_first("DarGeorefMethod"),
            "minimumElevationInMeters": get_first("DarMinimumElevationInMeters"),
            "maximumElevationInMeters": get_first("DarMaximumElevationInMeters"),
            # occurrence
            "lifeStage": get_first("DarLifeStage", "CardParasiteStage"),
            "catalogNumber": get_first("DarCatalogNumber", "RegRegistrationNumber"),
            "recordNumber": get_first("DarCollectorNumber"),
            "occurrenceID": get_first("AdmGUIDPreferredValue"),
            "recordedBy": get_all_non_person_strings(
                iter_all_values("CollEventFullNameSummaryData")
            ),
            "individualCount": get_individual_count(record),
            "sex": get_first("DarSex"),
            "preparations": get_first("DarPreparations"),
            # identification
            "typeStatus": get_first("DarTypeStatus", "sumTypeStatus"),
            "identifiedBy": get_first_non_person_string(
                iter_all_values("DarIdentifiedBy")
            ),
            "dateIdentified": get_first("EntIdeDateIdentified"),
            "identificationQualifier": get_first("DarIdentificationQualifier"),
            # taxon
            "scientificName": get_first_non_person_string(
                iter_all_values("DarScientificName")
            ),
            "scientificNameAuthorship": get_first_non_person_string(
                iter_all_values("IdeFiledAsAuthors")
            ),
            "kingdom": get_first("DarKingdom"),
            "phylum": get_first("DarPhylum"),
            "class": get_first("DarClass"),
            "order": get_first("DarOrder"),
            "family": get_first("DarFamily"),
            "genus": get_first("DarGenus"),
            "subgenus": get_first("DarSubgenus"),
            "specificEpithet": get_first("DarSpecies"),
            "infraspecificEpithet": get_first("DarSubspecies"),
            "higherClassification": get_first("DarHigherTaxon"),
            "taxonRank": get_first("DarInfraspecificRank"),
            # event
            "samplingProtocol": get_first("CollEventCollectionMethod"),
            "fieldNumber": get_first("DarFieldNumber"),
            "habitat": get_all("ColHabitatVerbatim"),
            "eventTime": get_first("DarTimeOfDay"),
            "day": get_first("DarDayCollected"),
            "month": get_first("DarMonthCollected"),
            "year": get_first("DarYearCollected"),
            # geological context
            "earliestEonOrLowestEonothem": get_first("DarEarliestEon"),
            "latestEonOrHighestEonothem": get_first("DarLatestEon"),
            "earliestEraOrLowestErathem": get_first("DarEarliestEra"),
            "latestEraOrHighestErathem": get_first("DarLatestEra"),
            "earliestPeriodOrLowestSystem": get_first("DarEarliestPeriod"),
            "latestPeriodOrHighestSystem": get_first("DarLatestPeriod"),
            "earliestEpochOrLowestSeries": get_first("DarEarliestEpoch"),
            "latestEpochOrHighestSeries": get_first("DarLatestEpoch"),
            "earliestAgeOrLowestStage": get_first("DarEarliestAge"),
            "latestAgeOrHighestStage": get_first("DarLatestAge"),
            "lowestBiostratigraphicZone": get_first("DarLowestBiostrat"),
            "highestBiostratigraphicZone": get_first("DarHighestBiostrat"),
            "group": get_first("DarGroup"),
            "formation": get_first("DarFormation"),
            "member": get_first("DarMember"),
            "bed": get_first("DarBed"),
            # custom
            "created": emu_date(
                get_first("AdmDateInserted"), get_first("AdmTimeInserted")
            ),
            "barcode": get_first("EntCatBarcode", "CardBarcode"),
            "preservative": get_first("CatPreservative", "EntCatPreservation"),
            "expedition": get_first("CollEventExpeditionName"),
            "vessel": get_first("CollEventVesselName"),
            "subDepartment": get_first("ColSubDepartment"),
            "partType": get_first("PrtType"),
            "registrationCode": get_first("RegCode"),
            "kindOfObject": get_first("CatKindOfObject"),
            "kindOfCollection": get_first("CatKindOfCollection"),
            "collectionKind": get_first("ColKind"),
            "collectionName": get_all("EntPriCollectionName"),
            "donorName": get_first("PalAcqAccLotDonorFullName"),
            "preparationType": get_first("DarPreparationType"),
            "observedWeight": get_first("DarObservedWeight"),
            "viceCounty": get_first("sumViceCounty"),
            "extractionMethod": get_first("DnaExtractionMethod"),
            "resuspendedIn": get_first("DnaReSuspendedIn"),
            "totalVolume": get_first("DnaTotalVolume"),
            "clutchSize": get_first("EggClutchSize"),
            "setMark": get_first("EggSetMark"),
            "nestShape": get_first("NesShape"),
            "nestSite": get_first("NesSite"),
            "populationCode": get_first("SilPopulationCode"),
            "exsiccata": get_first("CollExsiccati"),
            "exsiccataNumber": get_first("ColExsiccatiNumber"),
            "labelLocality": get_first("ColSiteDescription"),
            "plantDescription": get_all("ColPlantDescription"),
            "catalogueDescription": get_all("PalDesDescription"),
            "chronostratigraphy": get_first("PalStrChronostratLocal"),
            "lithostratigraphy": get_first("PalStrLithostratLocal"),
            "dateRegistered": get_first("MinDateRegistered"),
            "identificationAsRegistered": get_first("MinIdentificationAsRegistered"),
            "identificationDescription": get_all("MinIdentificationDescription"),
            "occurrence": get_first("MinPetOccurance"),
            "commodity": get_first("MinOreCommodity"),
            "depositType": get_first("MinOreDepositType"),
            "texture": get_all("MinTextureStructure"),
            "identificationVariety": get_first("MinIdentificationVariety"),
            "identificationOther": get_all("MinIdentificationOther"),
            "hostRock": get_first("MinHostRock"),
            "age": get_first("MinAgeDataAge"),
            "ageType": get_first("MinAgeDataType"),
            "tectonicProvince": get_first("MinNhmTectonicProvinceLocal"),
            "mine": get_first("MinNhmStandardMineLocal"),
            "miningDistrict": get_first("MinNhmMiningDistrictLocal"),
            "mineralComplex": get_first("MinNhmComplexLocal"),
            "geologyRegion": get_first("MinNhmRegionLocal"),
            "meteoriteType": get_first("MinMetType"),
            "meteoriteGroup": get_first("MinMetGroup"),
            "chondriteAchondrite": get_first("MinMetChondriteAchondrite"),
            "meteoriteClass": get_first("MinMetClass"),
            "petrologyType": get_first("MinMetPetType"),
            "petrologySubtype": get_first("MinMetPetSubtype"),
            "recovery": get_first("MinMetRecoveryFindFall"),
            "recoveryDate": get_first("MinMetRecoveryDate"),
            "recoveryWeight": get_first("MinMetRecoveryWeight"),
            # these need clean=False because each should return a tuple of the same
            # length where the values at each index align across all three tuples,
            # therefore we need to keep empty values
            "determinationTypes": get_all(
                "IdeCitationTypeStatus", clean=False, reduce=False
            ),
            "determinationNames": clean_determination_names(
                iter_all_values("EntIdeScientificNameLocal", clean=False)
            ),
            "determinationFiledAs": get_all("EntIdeFiledAs", clean=False, reduce=False),
            "project": get_all("NhmSecProjectName"),
        }
