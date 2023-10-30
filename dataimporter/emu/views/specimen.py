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
from dataimporter.model import SourceRecord
from dataimporter.view import View, FilterResult, SUCCESS_RESULT

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


class SpecimenView(View):
    """
    View for preparation records.

    This view populates the preparation resource on the Data Portal.
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
        # TODO: I had a note for checking minimumDepthInMeters and maximumDepthInMeters
        #       for some reason?

        # cache these for perf
        get_all = record.get_all_values
        get_first = record.get_first_value

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
            "verbatimLongitude": get_all("sumPreferredCentroidLongitude"),
            "verbatimLatitude": get_all("sumPreferredCentroidLatitude"),
            "locality": get_first(
                "PalNearestNamedPlaceLocal",
                "sumPreciseLocation",
                "MinNhmVerbatimLocalityLocal",
            ),
            "minimumDepthInMeters": get_first(
                "CollEventFromMetres", "DarMinimumDepthInMeters"
            ),
            "maximumDepthInMeters": get_first(
                "CollEventToMetres", "DarMaximumDepthInMeters"
            ),
            "country": get_all("DarCountry"),
            "waterBody": get_all("DarWaterBody"),
            "stateProvince": get_all("DarStateProvince"),
            "continent": get_all("DarContinent"),
            "island": get_all("DarIsland"),
            "islandGroup": get_all("DarIslandGroup"),
            "higherGeography": get_all("DarHigherGeography"),
            "geodeticDatum": get_all("DarGeodeticDatum"),
            "georeferenceProtocol": get_all("DarGeorefMethod"),
            "minimumElevationInMeters": get_all("DarMinimumElevationInMeters"),
            "maximumElevationInMeters": get_all("DarMaximumElevationInMeters"),
            # occurrence
            "lifeStage": get_first("DarLifeStage", "CardParasiteStage"),
            "catalogNumber": get_first("DarCatalogNumber", "RegRegistrationNumber"),
            "recordNumber": get_all("DarCollectorNumber"),
            "occurrenceID": get_first("AdmGUIDPreferredValue"),
            "recordedBy": get_first("DarCollector", "CollEventNameSummaryData"),
            "individualCount": get_all("DarIndividualCount"),
            "sex": get_all("DarSex"),
            "preparations": get_all("DarPreparations"),
            # identification
            "typeStatus": get_first("DarTypeStatus", "sumTypeStatus"),
            "identifiedBy": get_all("DarIdentifiedBy"),
            "dateIdentified": get_all("EntIdeDateIdentified"),
            "identificationQualifier": get_all("DarIdentificationQualifier"),
            # taxon
            "scientificName": get_all("DarScientificName"),
            "scientificNameAuthorship": get_all("IdeFiledAsAuthors"),
            "kingdom": get_all("DarKingdom"),
            "phylum": get_all("DarPhylum"),
            "class": get_all("DarClass"),
            "order": get_all("DarOrder"),
            "family": get_all("DarFamily"),
            "genus": get_all("DarGenus"),
            "subgenus": get_all("DarSubgenus"),
            "specificEpithet": get_all("DarSpecies"),
            "infraspecificEpithet": get_all("DarSubspecies"),
            "higherClassification": get_all("DarHigherTaxon"),
            "taxonRank": get_all("DarInfraspecificRank"),
            # event
            "samplingProtocol": get_all("CollEventCollectionMethod"),
            "fieldNumber": get_all("DarFieldNumber"),
            "habitat": get_all("ColHabitatVerbatim"),
            "eventTime": get_all("DarTimeOfDay"),
            "day": get_all("DarDayCollected"),
            "month": get_all("DarMonthCollected"),
            "year": get_all("DarYearCollected"),
            # geological context
            "earliestEonOrLowestEonothem": get_all("DarEarliestEon"),
            "latestEonOrHighestEonothem": get_all("DarLatestEon"),
            "earliestEraOrLowestErathem": get_all("DarEarliestEra"),
            "latestEraOrHighestErathem": get_all("DarLatestEra"),
            "earliestPeriodOrLowestSystem": get_all("DarEarliestPeriod"),
            "latestPeriodOrHighestSystem": get_all("DarLatestPeriod"),
            "earliestEpochOrLowestSeries": get_all("DarEarliestEpoch"),
            "latestEpochOrHighestSeries": get_all("DarLatestEpoch"),
            "earliestAgeOrLowestStage": get_all("DarEarliestAge"),
            "latestAgeOrHighestStage": get_all("DarLatestAge"),
            "lowestBiostratigraphicZone": get_all("DarLowestBiostrat"),
            "highestBiostratigraphicZone": get_all("DarHighestBiostrat"),
            "group": get_all("DarGroup"),
            "formation": get_all("DarFormation"),
            "member": get_all("DarMember"),
            "bed": get_all("DarBed"),
            # custom
            "created": emu_date(
                get_first("AdmDateInserted"), get_first("AdmTimeInserted")
            ),
            "barcode": get_first("EntCatBarcode", "CardBarcode"),
            "preservative": get_first("CatPreservative", "EntCatPreservation"),
            "expedition": get_first(
                "EntLocExpeditionNameLocal", "CollEventExpeditionName"
            ),
            "vessel": get_all("CollEventVesselName"),
            "subDepartment": get_all("ColSubDepartment"),
            "partType": get_all("PrtType"),
            "registrationCode": get_all("RegCode"),
            "kindOfObject": get_all("CatKindOfObject"),
            "kindOfCollection": get_all("CatKindOfCollection"),
            "collectionKind": get_all("ColKind"),
            "collectionName": get_all("EntPriCollectionName"),
            "donorName": get_all("PalAcqAccLotDonorFullName"),
            "preparationType": get_all("DarPreparationType"),
            "observedWeight": get_all("DarObservedWeight"),
            "viceCounty": get_all("sumViceCountry"),
            "extractionMethod": get_all("DnaExtractionMethod"),
            "resuspendedIn": get_all("DnaReSuspendedIn"),
            "totalVolume": get_all("DnaTotalVolume"),
            "clutchSize": get_all("EggClutchSize"),
            "setMark": get_all("EggSetMark"),
            "nestShape": get_all("NesShape"),
            "nestSite": get_all("NesSite"),
            "populationCode": get_all("SilPopulationCode"),
            "exsiccata": get_all("CollExsiccati"),
            "exsiccataNumber": get_all("ColExsiccatiNumber"),
            "labelLocality": get_all("ColSiteDescription"),
            "plantDescription": get_all("ColPlantDescription"),
            "catalogueDescription": get_all("PalDesDescription"),
            "chronostratigraphy": get_all("PalStrChronostratLocal"),
            "lithostratigraphy": get_all("PalStrLithostratLocal"),
            "dateRegistered": get_all("MinDateRegistered"),
            "identificationAsRegistered": get_all("MinIdentificationAsRegistered"),
            "identificationDescription": get_all("MinIdentificationDescription"),
            "occurrence": get_all("MinPetOccurance"),
            "commodity": get_all("MinOreCommodity"),
            "depositType": get_all("MinOreDepositType"),
            "texture": get_all("MinTextureStructure"),
            "identificationVariety": get_all("MinIdentificationVariety"),
            "identificationOther": get_all("MinIdentificationOther"),
            "hostRock": get_all("MinHostRock"),
            "age": get_all("MinAgeDataAge"),
            "ageType": get_all("MinAgeDataType"),
            "tectonicProvince": get_all("MinNhmTectonicProvinceLocal"),
            "mine": get_all("MinNhmStandardMineLocal"),
            "miningDistrict": get_all("MinNhmMiningDistrictLocal"),
            "mineralComplex": get_all("MinNhmComplexLocal"),
            "geologyRegion": get_all("MinNhmRegionLocal"),
            "meteoriteType": get_all("MinMetType"),
            "meteoriteGroup": get_all("MinMetGroup"),
            "chondriteAchondrite": get_all("MinMetChondriteAchondrite"),
            "meteoriteClass": get_all("MinMetClass"),
            "petrologyType": get_all("MinMetPetType"),
            "petrologySubtype": get_all("MinMetPetSubtype"),
            "recovery": get_all("MinMetRecoveryFindFall"),
            "recoveryDate": get_all("MinMetRecoveryDate"),
            "recoveryWeight": get_all("MinMetRecoveryWeight"),
            "registeredWeight": get_all("MinMetWeightAsRegistered"),
            "registeredWeightUnit": get_all("MinMetWeightAsRegisteredUnit"),
            "determinationTypes": get_all("IdeCitationTypeStatus"),
            "determinationNames": get_all("EntIdeScientificNameLocal"),
            "determinationFiledAs": get_all("EntIdeFiledAs"),
            "project": get_all("NhmSecProjectName"),
        }
