#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '14/02/2017'.
"""

from sqlalchemy import Column, String

from ke2sql.models.mixin import MixinModel
from ke2sql.models.base import Base


class CatalogueModel(Base, MixinModel):
    """
    Ecatalogue records
    """
    # Add extra field for recording record type
    record_type = Column(String, nullable=False)

    property_mappings = (
        # Record numbers
        ('AdmGUIDPreferredValue', 'occurrenceID'),
        ('DarCatalogNumber', 'catalogNumber'),
        # Used if DarCatalogueNumber is empty
        ('RegRegistrationNumber', 'catalogNumber'),
        # Taxonomy
        ('DarScientificName', 'scientificName'),
        # Rather than using the two darwin core fields DarScientificNameAuthorYear and ScientificNameAuthor
        # It's easier to just use IdeFiledAsAuthors which has them both concatenated
        ('IdeFiledAsAuthors', 'scientificNameAuthorship'),
        ('DarTypeStatus', 'typeStatus'),
        # If DarTypeStatus is empty, we'll use sumTypeStatus which has previous determinations
        ('sumTypeStatus', 'typeStatus'),
        # Use nearest name place rather than precise locality https://github.com/NaturalHistoryMuseum/ke2mongo/issues/29
        ('PalNearestNamedPlaceLocal', 'locality'),
        # Locality if nearest named place is empty
        # The encoding of DarLocality is buggered - see 1804973
        # So better to use the original field with the correct encoding
        ('sumPreciseLocation', 'locality'),
        # Locality if precise and nearest named place is empty
        ('MinNhmVerbatimLocalityLocal', 'locality'),
        ('DarCountry', 'country'),
        ('DarWaterBody', 'waterBody'),
        ('EntLocExpeditionNameLocal', 'expedition'),
        ('sumParticipantFullName', 'recordedBy'),
        ('ColDepartment', 'collectionCode'),
        # Taxonomy
        ('DarScientificName', 'scientificName'),
        ('DarKingdom', 'kingdom'),
        ('DarPhylum', 'phylum'),
        ('DarClass', 'class'),
        ('DarOrder', 'order'),
        ('DarFamily', 'family'),
        ('DarGenus', 'genus'),
        ('DarSubgenus', 'subgenus'),
        ('DarSpecies', 'specificEpithet'),
        ('DarSubspecies', 'infraspecificEpithet'),
        ('DarHigherTaxon', 'higherClassification'),
        ('DarInfraspecificRank', 'taxonRank'),
        # Location
        ('DarStateProvince', 'stateProvince'),
        ('DarContinent', 'continent'),
        ('DarIsland', 'island'),
        ('DarIslandGroup', 'islandGroup'),
        ('DarHigherGeography', 'higherGeography'),
        ('ColHabitatVerbatim', 'habitat'),
        ('DarDecimalLongitude', 'decimalLongitude'),
        ('DarDecimalLatitude', 'decimalLatitude'),
        ('sumPreferredCentroidLongitude', 'verbatimLongitude'),
        ('sumPreferredCentroidLatitude', 'verbatimLatitude'),
        ('DarGeodeticDatum', 'geodeticDatum'),
        ('DarGeorefMethod', 'georeferenceProtocol'),
        # Occurrence
        ('DarMinimumElevationInMeters', 'minimumElevationInMeters'),
        ('DarMaximumElevationInMeters', 'maximumElevationInMeters'),
        ('DarMinimumDepthInMeters', 'minimumDepthInMeters'),
        ('DarMaximumDepthInMeters', 'maximumDepthInMeters'),
        ('CollEventFromMetres', 'minimumDepthInMeters'),
        ('CollEventToMetres', 'maximumDepthInMeters'),
        ('DarCollectorNumber', 'recordNumber'),
        ('DarIndividualCount', 'individualCount'),
        # Duplicate lifeStage fields - only fields with a value get populated so
        # there is no risk in doing this
        ('DarLifeStage', 'lifeStage'),
        # Parasite cards use a different field for life stage
        ('CardParasiteStage', 'lifeStage'),
        ('DarSex', 'sex'),
        ('DarPreparations', 'preparations'),
        # Identification
        ('DarIdentifiedBy', 'identifiedBy'),
        # KE Emu has 3 fields for identification date: DarDayIdentified, DarMonthIdentified and DarYearIdentified
        # But EntIdeDateIdentified holds them all - which is what we want for dateIdentified
        ('EntIdeDateIdentified', 'dateIdentified'),
        ('DarIdentificationQualifier', 'identificationQualifier'),
        ('DarTimeOfDay', 'eventTime'),
        ('DarDayCollected', 'day'),
        ('DarMonthCollected', 'month'),
        ('DarYearCollected', 'year'),
        # Geology
        ('DarEarliestEon', 'earliestEonOrLowestEonothem'),
        ('DarLatestEon', 'latestEonOrHighestEonothem'),
        ('DarEarliestEra', 'earliestEraOrLowestErathem'),
        ('DarLatestEra', 'latestEraOrHighestErathem'),
        ('DarEarliestPeriod', 'earliestPeriodOrLowestSystem'),
        ('DarLatestPeriod', 'latestPeriodOrHighestSystem'),
        ('DarEarliestEpoch', 'earliestEpochOrLowestSeries'),
        ('DarLatestEpoch', 'latestEpochOrHighestSeries'),
        ('DarEarliestAge', 'earliestAgeOrLowestStage'),
        ('DarLatestAge', 'latestAgeOrHighestStage'),
        ('DarLowestBiostrat', 'lowestBiostratigraphicZone'),
        ('DarHighestBiostrat', 'highestBiostratigraphicZone'),
        ('DarGroup', 'group'),
        ('DarFormation', 'formation'),
        ('DarMember', 'member'),
        ('DarBed', 'bed'),
        # These fields do not map to DwC, but are still very useful
        ('ColRecordType', 'recordType'),
        ('ColSubDepartment', 'subDepartment'),
        ('PrtType', 'partType'),
        ('RegCode', 'registrationCode'),
        ('CatKindOfObject', 'kindOfObject'),
        ('CatKindOfCollection', 'kindOfCollection'),
        ('CatPreservative', 'preservative'),
        # Used if CatPreservative is empty
        ('EntCatPreservation', 'preservative'),
        ('ColKind', 'collectionKind'),
        ('EntPriCollectionName', 'collectionName'),
        ('PalAcqAccLotDonorFullName', 'donorName'),
        ('DarPreparationType', 'preparationType'),
        ('DarObservedWeight', 'observedWeight'),
        # Location
        # Data is stored in sumViceCountry field in ecatalogue data - but actually this
        # should be viceCountry (which it is in esites)
        ('sumViceCountry', 'viceCounty'),
        # DNA
        ('DnaExtractionMethod', 'extractionMethod'),
        ('DnaReSuspendedIn', 'resuspendedIn'),
        ('DnaTotalVolume', 'totalVolume'),
        # Parasite card
        ('EntCatBarcode', 'barcode'),
        ('CardBarcode', 'barcode'),
        # Egg
        ('EggClutchSize', 'clutchSize'),
        ('EggSetMark', 'setMark'),
        # Nest
        ('NesShape', 'nestShape'),
        ('NesSite', 'nestSite'),
        # Silica gel
        ('SilPopulationCode', 'populationCode'),
        # Botany
        ('CollExsiccati', 'exsiccati'),
        ('ColExsiccatiNumber', 'exsiccatiNumber'),
        ('ColSiteDescription', 'labelLocality'),
        ('ColPlantDescription', 'plantDescription'),
        ('FeaCultivated', 'cultivated'),
        # Paleo
        ('PalDesDescription', 'catalogueDescription'),
        ('PalStrChronostratLocal', 'chronostratigraphy'),
        ('PalStrLithostratLocal', 'lithostratigraphy'),
        # Mineralogy
        ('MinDateRegistered', 'dateRegistered'),
        ('MinIdentificationAsRegistered', 'identificationAsRegistered'),
        ('MinIdentificationDescription', 'identificationDescription'),
        ('MinPetOccurance', 'occurrence'),
        ('MinOreCommodity', 'commodity'),
        ('MinOreDepositType', 'depositType'),
        ('MinTextureStructure', 'texture'),
        ('MinIdentificationVariety', 'identificationVariety'),
        ('MinIdentificationOther', 'identificationOther'),
        ('MinHostRock', 'hostRock'),
        ('MinAgeDataAge', 'age'),
        ('MinAgeDataType', 'ageType'),
        # Mineralogy location
        ('MinNhmTectonicProvinceLocal', 'tectonicProvince'),
        ('MinNhmStandardMineLocal', 'mine'),
        ('MinNhmMiningDistrictLocal', 'miningDistrict'),
        ('MinNhmComplexLocal', 'mineralComplex'),
        ('MinNhmRegionLocal', 'geologyRegion'),
        # Meteorite
        ('MinMetType', 'meteoriteType'),
        ('MinMetGroup', 'meteoriteGroup'),
        ('MinMetChondriteAchondrite', 'chondriteAchondrite'),
        ('MinMetClass', 'meteoriteClass'),
        ('MinMetPetType', 'petrologyType'),
        ('MinMetPetSubtype', 'petrologySubtype'),
        ('MinMetRecoveryFindFall', 'recovery'),
        ('MinMetRecoveryDate', 'recoveryDate'),
        ('MinMetRecoveryWeight', 'recoveryWeight'),
        ('MinMetWeightAsRegistered', 'registeredWeight'),
        ('MinMetWeightAsRegisteredUnit', 'registeredWeightUnit'),
        # Project
        ('NhmSecProjectName', 'project'),

        # Internal
        # ('RegRegistrationParentRef', '_parentRef', 'int32'),
        # ('CardParasiteRef', '_cardParasiteRef', 'int32'),
        # ('IdeCitationTypeStatus', '_determinationTypes'),
        # ('EntIdeScientificNameLocal', '_determinationNames', 'string:250'),
        # ('EntIdeFiledAs', '_determinationFiledAs'),

    )

    # List of record types (ColRecordType) to exclude from the import
    excluded_types = [
        'Acquisition',
        'Bound Volume',
        'Bound Volume Page',
        'Collection Level Description',
        'DNA Card',  # 1 record, but keep an eye on this
        'Field Notebook',
        'Field Notebook (Double Page)',
        'Image',
        'Image (electronic)',
        'Image (non-digital)',
        'Image (digital)',
        'Incoming Loan',
        'L&A Catalogue',
        'Missing',
        'Object Entry',
        'object entry',  # FFS
        'Object entry',  # FFFS
        'PEG Specimen',
        'PEG Catalogue',
        'Preparation',
        'Rack File',
        'Tissue',  # Only 2 records. Watch.
        'Transient Lot'
    ]

    # List of record statuses (SecRecordStatus) to exclude from the import
    excluded_statuses = [
        "DELETE",
        "DELETE-MERGED",
        "DUPLICATION",
        "Disposed of",
        "FROZEN ARK",
        "INVALID",
        "POSSIBLE TYPE",
        "PROBLEM",
        "Re-registered in error",
        "Reserved",
        "Retired",
        "Retired (see Notes)",
        "Retired (see Notes)Retired (see Notes)",
        "SCAN_cat",
        "See Notes",
        "Specimen missing - see notes",
        "Stub",
        "Stub Record",
        "Stub record"
    ]

    # Record must be in one of these collection departments
    collection_departments = [
        "Botany",
        "Entomology",
        "Mineralogy",
        "Palaeontology",
        "Zoology"
    ]

    def is_importable(self, record):
        """
        Evaluate whether a record is importable
        At the very least a record will need AdmPublishWebNoPasswordFlag set to Y,
        Additional models will extend this to provide additional filters
        :param record:
        :return: boolean - false if not importable
        """

        # Records must have a GUID
        if not getattr(record, 'AdmGUIDPreferredValue', None):
            return False

        # Does this record have an excluded status - Stub etc.,
        record_status = getattr(record, 'SecRecordStatus', None)
        if record_status in self.excluded_statuses:
            return False

        # Does this record have an excluded status - Stub etc.,
        record_type = getattr(record, 'ColRecordType', None)
        if record_type in self.excluded_types:
            return False

        # Record must be in one of the known collection departments
        # (Otherwise the home page breaks)
        collection_department = getattr(record, 'ColDepartment', None)
        if collection_department not in self.collection_departments:
            return False

        # Run the record passed the base filter (checks AdmPublishWebNoPasswordFlag)
        return super(CatalogueModel, self).is_importable(record)

