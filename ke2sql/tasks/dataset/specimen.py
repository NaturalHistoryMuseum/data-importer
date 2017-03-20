#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '14/03/2017'.



"""

import luigi

from ke2sql.tasks.dataset.base import BaseDatasetTask


class SpecimenDatasetTask(BaseDatasetTask):

    record_types = [
        'Specimen',
        'DNA Prep',
        'Specimen - single',
        'Mammal Group Parent',
        'Mammal Group Part',
        'Bryozoa Part Specimen',
        'Silica Gel Specimen'
    ]

    package_name = 'collection-specimens'
    package_description = "Specimen records from the Natural History Museum\'s collection"
    package_title = "Collection specimens"

    resource_title = 'Specimen records'
    resource_description = 'Specimen records'

    indexed_fields = ['collectionCode', 'catalogNumber', 'created', 'project']

    fields = [
        # Update fields
        ('ecatalogue.AdmDateModified', 'dateModified'),
        ('ecatalogue.AdmDateInserted', 'dateCreated'),
        ('ecatalogue.AdmGUIDPreferredValue', 'occurrenceID'),
        # Record numbers
        ('ecatalogue.DarCatalogNumber', 'catalogNumber'),
        # Used if DarCatalogueNumber is empty
        ('ecatalogue.RegRegistrationNumber', 'catalogNumber'),
        # Taxonomy
        ('ecatalogue.DarScientificName', 'scientificName'),
        # Rather than using the two darwin core fields DarScientificNameAuthorYear and ScientificNameAuthor
        # It's easier to just use IdeFiledAsAuthors which has them both concatenated
        ('ecatalogue.IdeFiledAsAuthors', 'scientificNameAuthorship'),
        ('ecatalogue.DarTypeStatus', 'typeStatus'),
        # If DarTypeStatus is empty, we'll use sumTypeStatus which has previous determinations
        ('ecatalogue.sumTypeStatus', 'typeStatus'),
        # Use nearest name place rather than precise locality https://github.com/NaturalHistoryMuseum/ke2mongo/issues/29
        ('ecatalogue.PalNearestNamedPlaceLocal', 'locality'),
        # Locality if nearest named place is empty
        # The encoding of DarLocality is buggered - see 1804973
        # So better to use the original field with the correct encoding
        ('ecatalogue.sumPreciseLocation', 'locality'),
        # Locality if precise and nearest named place is empty
        ('ecatalogue.MinNhmVerbatimLocalityLocal', 'locality'),
        ('ecatalogue.DarCountry', 'country'),
        ('ecatalogue.DarWaterBody', 'waterBody'),
        ('ecatalogue.EntLocExpeditionNameLocal', 'expedition'),
        ('ecatalogue.sumParticipantFullName', 'recordedBy'),
        ('ecatalogue.ColDepartment', 'collectionCode'),
        # Taxonomy
        ('ecatalogue.DarScientificName', 'scientificName'),
        ('ecatalogue.DarKingdom', 'kingdom'),
        ('ecatalogue.DarPhylum', 'phylum'),
        ('ecatalogue.DarClass', 'class'),
        ('ecatalogue.DarOrder', 'order'),
        ('ecatalogue.DarFamily', 'family'),
        ('ecatalogue.DarGenus', 'genus'),
        ('ecatalogue.DarSubgenus', 'subgenus'),
        ('ecatalogue.DarSpecies', 'specificEpithet'),
        ('ecatalogue.DarSubspecies', 'infraspecificEpithet'),
        ('ecatalogue.DarHigherTaxon', 'higherClassification'),
        ('ecatalogue.DarInfraspecificRank', 'taxonRank'),
        # Location
        ('ecatalogue.DarStateProvince', 'stateProvince'),
        ('ecatalogue.DarContinent', 'continent'),
        ('ecatalogue.DarIsland', 'island'),
        ('ecatalogue.DarIslandGroup', 'islandGroup'),
        ('ecatalogue.DarHigherGeography', 'higherGeography'),
        ('ecatalogue.ColHabitatVerbatim', 'habitat'),
        ('ecatalogue.DarDecimalLongitude', 'decimalLongitude'),
        ('ecatalogue.DarDecimalLatitude', 'decimalLatitude'),
        ('ecatalogue.sumPreferredCentroidLongitude', 'verbatimLongitude'),
        ('ecatalogue.sumPreferredCentroidLatitude', 'verbatimLatitude'),
        ('ecatalogue.DarGeodeticDatum', 'geodeticDatum'),
        ('ecatalogue.DarGeorefMethod', 'georeferenceProtocol'),
        # Occurrence
        ('ecatalogue.DarMinimumElevationInMeters', 'minimumElevationInMeters'),
        ('ecatalogue.DarMaximumElevationInMeters', 'maximumElevationInMeters'),
        ('ecatalogue.DarMinimumDepthInMeters', 'minimumDepthInMeters'),
        ('ecatalogue.DarMaximumDepthInMeters', 'maximumDepthInMeters'),
        ('ecatalogue.CollEventFromMetres', 'minimumDepthInMeters'),
        ('ecatalogue.CollEventToMetres', 'maximumDepthInMeters'),
        ('ecatalogue.DarCollectorNumber', 'recordNumber'),
        ('ecatalogue.DarIndividualCount', 'individualCount'),
        # Duplicate lifeStage fields - only fields with a value get populated so
        # there is no risk in doing this
        ('ecatalogue.DarLifeStage', 'lifeStage'),
        # Parasite cards use a different field for life stage
        ('ecatalogue.CardParasiteStage', 'lifeStage'),
        ('ecatalogue.DarSex', 'sex'),
        ('ecatalogue.DarPreparations', 'preparations'),
        # Identification
        ('ecatalogue.DarIdentifiedBy', 'identifiedBy'),
        # KE Emu has 3 fields for identification date: DarDayIdentified, DarMonthIdentified and DarYearIdentified
        # But EntIdeDateIdentified holds them all - which is what we want for dateIdentified
        ('ecatalogue.EntIdeDateIdentified', 'dateIdentified'),
        ('ecatalogue.DarIdentificationQualifier', 'identificationQualifier'),
        ('ecatalogue.DarTimeOfDay', 'eventTime'),
        ('ecatalogue.DarDayCollected', 'day'),
        ('ecatalogue.DarMonthCollected', 'month'),
        ('ecatalogue.DarYearCollected', 'year'),
        # Geology
        ('ecatalogue.DarEarliestEon', 'earliestEonOrLowestEonothem'),
        ('ecatalogue.DarLatestEon', 'latestEonOrHighestEonothem'),
        ('ecatalogue.DarEarliestEra', 'earliestEraOrLowestErathem'),
        ('ecatalogue.DarLatestEra', 'latestEraOrHighestErathem'),
        ('ecatalogue.DarEarliestPeriod', 'earliestPeriodOrLowestSystem'),
        ('ecatalogue.DarLatestPeriod', 'latestPeriodOrHighestSystem'),
        ('ecatalogue.DarEarliestEpoch', 'earliestEpochOrLowestSeries'),
        ('ecatalogue.DarLatestEpoch', 'latestEpochOrHighestSeries'),
        ('ecatalogue.DarEarliestAge', 'earliestAgeOrLowestStage'),
        ('ecatalogue.DarLatestAge', 'latestAgeOrHighestStage'),
        ('ecatalogue.DarLowestBiostrat', 'lowestBiostratigraphicZone'),
        ('ecatalogue.DarHighestBiostrat', 'highestBiostratigraphicZone'),
        ('ecatalogue.DarGroup', 'group'),
        ('ecatalogue.DarFormation', 'formation'),
        ('ecatalogue.DarMember', 'member'),
        ('ecatalogue.DarBed', 'bed'),
        # These fields do not map to DwC, but are still very useful
        ('ecatalogue.ColSubDepartment', 'subDepartment'),
        ('ecatalogue.PrtType', 'partType'),
        ('ecatalogue.RegCode', 'registrationCode'),
        ('ecatalogue.CatKindOfObject', 'kindOfObject'),
        ('ecatalogue.CatKindOfCollection', 'kindOfCollection'),
        ('ecatalogue.CatPreservative', 'preservative'),
        # Used if CatPreservative is empty
        ('ecatalogue.EntCatPreservation', 'preservative'),
        ('ecatalogue.ColKind', 'collectionKind'),
        ('ecatalogue.EntPriCollectionName', 'collectionName'),
        ('ecatalogue.PalAcqAccLotDonorFullName', 'donorName'),
        ('ecatalogue.DarPreparationType', 'preparationType'),
        ('ecatalogue.DarObservedWeight', 'observedWeight'),
        # Location
        # Data is stored in sumViceCountry field in ecatalogue data - but actually this
        # should be viceCountry (which it is in esites)
        ('ecatalogue.sumViceCountry', 'viceCounty'),
        # DNA
        ('ecatalogue.DnaExtractionMethod', 'extractionMethod'),
        ('ecatalogue.DnaReSuspendedIn', 'resuspendedIn'),
        ('ecatalogue.DnaTotalVolume', 'totalVolume'),
        # Parasite card
        ('ecatalogue.EntCatBarcode', 'barcode'),
        ('ecatalogue.CardBarcode', 'barcode'),
        # Egg
        ('ecatalogue.EggClutchSize', 'clutchSize'),
        ('ecatalogue.EggSetMark', 'setMark'),
        # Nest
        ('ecatalogue.NesShape', 'nestShape'),
        ('ecatalogue.NesSite', 'nestSite'),
        # Silica gel
        ('ecatalogue.SilPopulationCode', 'populationCode'),
        # Botany
        ('ecatalogue.CollExsiccati', 'exsiccati'),
        ('ecatalogue.ColExsiccatiNumber', 'exsiccatiNumber'),
        ('ecatalogue.ColSiteDescription', 'labelLocality'),
        ('ecatalogue.ColPlantDescription', 'plantDescription'),
        ('ecatalogue.FeaCultivated', 'cultivated'),
        # Paleo
        ('ecatalogue.PalDesDescription', 'catalogueDescription'),
        ('ecatalogue.PalStrChronostratLocal', 'chronostratigraphy'),
        ('ecatalogue.PalStrLithostratLocal', 'lithostratigraphy'),
        # Mineralogy
        ('ecatalogue.MinDateRegistered', 'dateRegistered'),
        ('ecatalogue.MinIdentificationAsRegistered', 'identificationAsRegistered'),
        ('ecatalogue.MinIdentificationDescription', 'identificationDescription'),
        ('ecatalogue.MinPetOccurance', 'occurrence'),
        ('ecatalogue.MinOreCommodity', 'commodity'),
        ('ecatalogue.MinOreDepositType', 'depositType'),
        ('ecatalogue.MinTextureStructure', 'texture'),
        ('ecatalogue.MinIdentificationVariety', 'identificationVariety'),
        ('ecatalogue.MinIdentificationOther', 'identificationOther'),
        ('ecatalogue.MinHostRock', 'hostRock'),
        ('ecatalogue.MinAgeDataAge', 'age'),
        ('ecatalogue.MinAgeDataType', 'ageType'),
        # Mineralogy location
        ('ecatalogue.MinNhmTectonicProvinceLocal', 'tectonicProvince'),
        ('ecatalogue.MinNhmStandardMineLocal', 'mine'),
        ('ecatalogue.MinNhmMiningDistrictLocal', 'miningDistrict'),
        ('ecatalogue.MinNhmComplexLocal', 'mineralComplex'),
        ('ecatalogue.MinNhmRegionLocal', 'geologyRegion'),
        # Meteorite
        ('ecatalogue.MinMetType', 'meteoriteType'),
        ('ecatalogue.MinMetGroup', 'meteoriteGroup'),
        ('ecatalogue.MinMetChondriteAchondrite', 'chondriteAchondrite'),
        ('ecatalogue.MinMetClass', 'meteoriteClass'),
        ('ecatalogue.MinMetPetType', 'petrologyType'),
        ('ecatalogue.MinMetPetSubtype', 'petrologySubtype'),
        ('ecatalogue.MinMetRecoveryFindFall', 'recovery'),
        ('ecatalogue.MinMetRecoveryDate', 'recoveryDate'),
        ('ecatalogue.MinMetRecoveryWeight', 'recoveryWeight'),
        ('ecatalogue.MinMetWeightAsRegistered', 'registeredWeight'),
        ('ecatalogue.MinMetWeightAsRegisteredUnit', 'registeredWeightUnit'),
        # Determinations
        ('ecatalogue.IdeCitationTypeStatus', 'determinationTypes'),
        ('ecatalogue.EntIdeScientificNameLocal', 'determinationNames'),
        ('ecatalogue.EntIdeFiledAs', 'determinationFiledAs'),
        # Project
        ('ecatalogue.NhmSecProjectName', 'project'),

        # Extra column field mappings - not included in properties, have a column in their own right
        ('ecatalogue.MulMultiMediaRef', 'multimedia_irns'),
        ('ecatalogue.ColRecordType', 'record_type'),
        ('ecatalogue.EntIndIndexLotTaxonNameLocalRef', 'indexlot_taxonomy_irn'),
        ('ecatalogue.RegRegistrationParentRef', 'parent_irn'),
        ('ecatalogue.CardParasiteRef', 'parasite_taxonomy_irn'),
        # Populate embargo date
        # Will use NhmSecEmbargoExtensionDate if set; otherwise NhmSecEmbargoDate
        ('ecatalogue.NhmSecEmbargoDate', 'embargo_date'),
        ('ecatalogue.NhmSecEmbargoExtensionDate', 'embargo_date'),
    ]

    joins = [
        # ('etaxonomy', 'indexlot_taxonomy_irn')
    ]

if __name__ == "__main__":
    luigi.run(main_task_cls=SpecimenDatasetTask)
