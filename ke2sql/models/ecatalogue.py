
import luigi

from ke2sql.tasks.bulk import BaseTask


class ECatalogueTask(BaseTask):

    # [
    #
    # ]
    #
    # ("record_type", "TEXT"),
    # ("multimedia_irns", "ARRAY[INTEGER]"),  # Bit flaky
    # ("multimedia_irn", "INTEGER REFERENCES products (product_no),"),

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

if __name__ == '__main__':
    luigi.run(main_task_cls=ECatalogueTask)
