#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '14/03/2017'.



"""

import luigi
from operator import is_not
from data_importer.tasks.dataset import DatasetTask
from data_importer.lib.operators import is_one_of, is_not_one_of
from data_importer.lib.field import Field
from data_importer.lib.foreign_key import ForeignKeyField
from data_importer.lib.filter import Filter
from data_importer.lib.config import Config


class SpecimenDatasetTask(DatasetTask):
    package_name = 'collection-specimens'
    package_description = "Specimen records from the Natural History Museum\'s collection"
    package_title = "Collection specimens"

    resource_title = 'Specimen records'
    resource_id = Config.get('resource_ids', 'specimen')
    resource_description = 'Specimen records'
    resource_type = 'dwc'  # Darwin Core

    record_types = [
        'Specimen',
        'Specimen - single',
        "Specimen - multiple",
        'DNA Prep',
        'Mammal Group Parent',  # Not included in the actual output; added to part records
        'Mammal Group Part',
        "Bird Group Parent",
        "Bird Group Part",
        'Bryozoa Part Specimen',
        'Silica Gel Specimen',
        "Parasite Card",
    ]

    fields = DatasetTask.fields + [
        # Record numbers & metadata
        # Use RegRegistrationNumber if DarCatalogueNumber is empty
        Field('ecatalogue', ['DarCatalogNumber', 'RegRegistrationNumber'], 'catalogNumber'),
        Field('ecatalogue', 'irn', 'otherCatalogNumbers', lambda irn: 'NHMUK:ecatalogue:%s' % irn),
        Field('ecatalogue', 'ColDepartment', 'collectionCode', lambda dept: 'BMNH(E)' if dept == 'Entomology' else dept[:3].upper()),

        # Taxonomy
        Field('ecatalogue', 'DarScientificName', 'scientificName'),
        # Rather than using the two darwin core fields DarScientificNameAuthorYear and ScientificNameAuthor
        # It's easier to just use IdeFiledAsAuthors which has them both concatenated
        Field('ecatalogue', 'IdeFiledAsAuthors', 'scientificNameAuthorship'),
        # If DarTypeStatus is empty, we'll use sumTypeStatus which has previous determinations
        Field('ecatalogue', ['DarTypeStatus', 'sumTypeStatus'], 'typeStatus'),
        Field('ecatalogue', 'DarKingdom', 'kingdom'),
        Field('ecatalogue', 'DarPhylum', 'phylum'),
        Field('ecatalogue', 'DarClass', 'class'),
        Field('ecatalogue', 'DarOrder', 'order'),
        Field('ecatalogue', 'DarFamily', 'family'),
        Field('ecatalogue', 'DarGenus', 'genus'),
        Field('ecatalogue', 'DarSubgenus', 'subgenus'),
        Field('ecatalogue', 'DarSpecies', 'specificEpithet'),
        Field('ecatalogue', 'DarSubspecies', 'infraspecificEpithet'),
        Field('ecatalogue', 'DarHigherTaxon', 'higherClassification'),
        Field('ecatalogue', 'DarInfraspecificRank', 'taxonRank'),

        # Location
        # Use nearest name place rather than precise locality https://github', 'com/NaturalHistoryMuseum/ke2mongo/issues/29
        Field('ecatalogue', ['PalNearestNamedPlaceLocal', 'sumPreciseLocation', 'MinNhmVerbatimLocalityLocal'], 'locality'),
        Field('ecatalogue', 'DarCountry', 'country'),
        Field('ecatalogue', 'DarWaterBody', 'waterBody'),
        Field('ecatalogue', ['EntLocExpeditionNameLocal', 'CollEventExpeditionName'], 'expedition'),
        Field('ecatalogue', 'CollEventVesselName', 'vessel'),
        Field('ecatalogue', ['DarCollector', 'CollEventNameSummaryData'], 'recordedBy'),
        Field('ecatalogue', 'CollEventCollectionMethod', 'samplingProtocol'),
        Field('ecatalogue', 'DarFieldNumber', 'fieldNumber'),
        Field('ecatalogue', 'DarStateProvince', 'stateProvince'),
        Field('ecatalogue', 'DarContinent', 'continent'),
        Field('ecatalogue', 'DarIsland', 'island'),
        Field('ecatalogue', 'DarIslandGroup', 'islandGroup'),
        Field('ecatalogue', 'DarHigherGeography', 'higherGeography'),
        Field('ecatalogue', 'ColHabitatVerbatim', 'habitat'),
        Field('ecatalogue', 'DarDecimalLongitude', 'decimalLongitude'),
        Field('ecatalogue', 'DarDecimalLatitude', 'decimalLatitude'),
        Field('ecatalogue', 'sumPreferredCentroidLongitude', 'verbatimLongitude'),
        Field('ecatalogue', 'sumPreferredCentroidLatitude', 'verbatimLatitude'),
        # If sumPreferredCentroidLatDec is populated, lat/lng is centroid
        Field('ecatalogue', 'sumPreferredCentroidLatDec', 'centroid', lambda v: True if v else False),
        Field('ecatalogue', 'DarGeodeticDatum', 'geodeticDatum'),
        Field('ecatalogue', 'DarGeorefMethod', 'georeferenceProtocol'),
        # Occurrence
        Field('ecatalogue', 'DarMinimumElevationInMeters', 'minimumElevationInMeters'),
        Field('ecatalogue', 'DarMaximumElevationInMeters', 'maximumElevationInMeters'),
        Field('ecatalogue', ['DarMinimumDepthInMeters', 'CollEventFromMetres'], 'minimumDepthInMeters'),
        Field('ecatalogue', ['DarMaximumDepthInMeters', 'CollEventToMetres'], 'maximumDepthInMeters'),
        Field('ecatalogue', 'DarCollectorNumber', 'recordNumber'),
        Field('ecatalogue', 'DarIndividualCount', 'individualCount'),
        # Parasite cards use a different field for life stage
        Field('ecatalogue', ['DarLifeStage', 'CardParasiteStage'], 'lifeStage'),
        Field('ecatalogue', 'DarSex', 'sex'),
        Field('ecatalogue', 'DarPreparations', 'preparations'),
        # Identification
        Field('ecatalogue', 'DarIdentifiedBy', 'identifiedBy'),
        # KE Emu has 3 fields for identification date: DarDayIdentified, DarMonthIdentified and DarYearIdentified
        # But EntIdeDateIdentified holds them all - which is what we want for dateIdentified
        Field('ecatalogue', 'EntIdeDateIdentified', 'dateIdentified'),
        Field('ecatalogue', 'DarIdentificationQualifier', 'identificationQualifier'),
        Field('ecatalogue', 'DarTimeOfDay', 'eventTime'),
        Field('ecatalogue', 'DarDayCollected', 'day'),
        Field('ecatalogue', 'DarMonthCollected', 'month'),
        Field('ecatalogue', 'DarYearCollected', 'year'),
        # Geology
        Field('ecatalogue', 'DarEarliestEon', 'earliestEonOrLowestEonothem'),
        Field('ecatalogue', 'DarLatestEon', 'latestEonOrHighestEonothem'),
        Field('ecatalogue', 'DarEarliestEra', 'earliestEraOrLowestErathem'),
        Field('ecatalogue', 'DarLatestEra', 'latestEraOrHighestErathem'),
        Field('ecatalogue', 'DarEarliestPeriod', 'earliestPeriodOrLowestSystem'),
        Field('ecatalogue', 'DarLatestPeriod', 'latestPeriodOrHighestSystem'),
        Field('ecatalogue', 'DarEarliestEpoch', 'earliestEpochOrLowestSeries'),
        Field('ecatalogue', 'DarLatestEpoch', 'latestEpochOrHighestSeries'),
        Field('ecatalogue', 'DarEarliestAge', 'earliestAgeOrLowestStage'),
        Field('ecatalogue', 'DarLatestAge', 'latestAgeOrHighestStage'),
        Field('ecatalogue', 'DarLowestBiostrat', 'lowestBiostratigraphicZone'),
        Field('ecatalogue', 'DarHighestBiostrat', 'highestBiostratigraphicZone'),
        Field('ecatalogue', 'DarGroup', 'group'),
        Field('ecatalogue', 'DarFormation', 'formation'),
        Field('ecatalogue', 'DarMember', 'member'),
        Field('ecatalogue', 'DarBed', 'bed'),
        # These fields do not map to DwC, but are still very useful
        Field('ecatalogue', 'ColSubDepartment', 'subDepartment'),
        Field('ecatalogue', 'PrtType', 'partType'),
        Field('ecatalogue', 'RegCode', 'registrationCode'),
        Field('ecatalogue', 'CatKindOfObject', 'kindOfObject'),
        Field('ecatalogue', 'CatKindOfCollection', 'kindOfCollection'),
        Field('ecatalogue', ['CatPreservative', 'EntCatPreservation'], 'preservative'),
        Field('ecatalogue', 'ColKind', 'collectionKind'),
        Field('ecatalogue', 'EntPriCollectionName', 'collectionName'),
        Field('ecatalogue', 'PalAcqAccLotDonorFullName', 'donorName'),
        Field('ecatalogue', 'DarPreparationType', 'preparationType'),
        Field('ecatalogue', 'DarObservedWeight', 'observedWeight'),
        # Location
        # Data is stored in sumViceCountry field in ecatalogue data - but actually this
        # should be viceCountry Field(which it is in esites)
        Field('ecatalogue', 'sumViceCountry', 'viceCounty'),
        # DNA
        Field('ecatalogue', 'DnaExtractionMethod', 'extractionMethod'),
        Field('ecatalogue', 'DnaReSuspendedIn', 'resuspendedIn'),
        Field('ecatalogue', 'DnaTotalVolume', 'totalVolume'),
        # Parasite card
        Field('ecatalogue', ['EntCatBarcode', 'CardBarcode'], 'barcode'),
        # Egg
        Field('ecatalogue', 'EggClutchSize', 'clutchSize'),
        Field('ecatalogue', 'EggSetMark', 'setMark'),
        # Nest
        Field('ecatalogue', 'NesShape', 'nestShape'),
        Field('ecatalogue', 'NesSite', 'nestSite'),
        # Silica gel
        Field('ecatalogue', 'SilPopulationCode', 'populationCode'),
        # Botany
        Field('ecatalogue', 'CollExsiccati', 'exsiccati'),
        Field('ecatalogue', 'ColExsiccatiNumber', 'exsiccatiNumber'),
        Field('ecatalogue', 'ColSiteDescription', 'labelLocality'),
        Field('ecatalogue', 'ColPlantDescription', 'plantDescription'),
        # Paleo
        Field('ecatalogue', 'PalDesDescription', 'catalogueDescription'),
        Field('ecatalogue', 'PalStrChronostratLocal', 'chronostratigraphy'),
        Field('ecatalogue', 'PalStrLithostratLocal', 'lithostratigraphy'),
        # Mineralogy
        Field('ecatalogue', 'MinDateRegistered', 'dateRegistered'),
        Field('ecatalogue', 'MinIdentificationAsRegistered', 'identificationAsRegistered'),
        Field('ecatalogue', 'MinIdentificationDescription', 'identificationDescription'),
        Field('ecatalogue', 'MinPetOccurance', 'occurrence'),
        Field('ecatalogue', 'MinOreCommodity', 'commodity'),
        Field('ecatalogue', 'MinOreDepositType', 'depositType'),
        Field('ecatalogue', 'MinTextureStructure', 'texture'),
        Field('ecatalogue', 'MinIdentificationVariety', 'identificationVariety'),
        Field('ecatalogue', 'MinIdentificationOther', 'identificationOther'),
        Field('ecatalogue', 'MinHostRock', 'hostRock'),
        Field('ecatalogue', 'MinAgeDataAge', 'age'),
        Field('ecatalogue', 'MinAgeDataType', 'ageType'),
        # Mineralogy location
        Field('ecatalogue', 'MinNhmTectonicProvinceLocal', 'tectonicProvince'),
        Field('ecatalogue', 'MinNhmStandardMineLocal', 'mine'),
        Field('ecatalogue', 'MinNhmMiningDistrictLocal', 'miningDistrict'),
        Field('ecatalogue', 'MinNhmComplexLocal', 'mineralComplex'),
        Field('ecatalogue', 'MinNhmRegionLocal', 'geologyRegion'),
        # Meteorite
        Field('ecatalogue', 'MinMetType', 'meteoriteType'),
        Field('ecatalogue', 'MinMetGroup', 'meteoriteGroup'),
        Field('ecatalogue', 'MinMetChondriteAchondrite', 'chondriteAchondrite'),
        Field('ecatalogue', 'MinMetClass', 'meteoriteClass'),
        Field('ecatalogue', 'MinMetPetType', 'petrologyType'),
        Field('ecatalogue', 'MinMetPetSubtype', 'petrologySubtype'),
        Field('ecatalogue', 'MinMetRecoveryFindFall', 'recovery'),
        Field('ecatalogue', 'MinMetRecoveryDate', 'recoveryDate'),
        Field('ecatalogue', 'MinMetRecoveryWeight', 'recoveryWeight'),
        Field('ecatalogue', 'MinMetWeightAsRegistered', 'registeredWeight'),
        Field('ecatalogue', 'MinMetWeightAsRegisteredUnit', 'registeredWeightUnit'),
        # Determinations
        Field('ecatalogue', 'IdeCitationTypeStatus', 'determinationTypes'),
        Field('ecatalogue', 'EntIdeScientificNameLocal', 'determinationNames'),
        Field('ecatalogue', 'EntIdeFiledAs', 'determinationFiledAs'),
        # Project
        Field('ecatalogue', 'NhmSecProjectName', 'project'),
    ]

    foreign_keys = DatasetTask.foreign_keys + [
        ForeignKeyField('ecatalogue', 'ecatalogue', 'RegRegistrationParentRef', join_alias='parent_catalogue'),
        ForeignKeyField('ecatalogue', 'etaxonomy', 'CardParasiteRef'),
    ]

    def create_table(self, connection):
        """
        For the windshaft map, we need a geospatial table
        @param connection:
        @return:
        """
        query = """
        CREATE TABLE "{table}" AS (SELECT ecatalogue.irn as _id,
            cast(ecatalogue.properties->>'decimalLatitude' as FLOAT8) as "decimalLatitude",
            cast(ecatalogue.properties->>'decimalLongitude' as FLOAT8) as "decimalLongitude",
            st_setsrid(st_makepoint(
                cast(ecatalogue.properties->>'decimalLongitude' as FLOAT8),
                cast(ecatalogue.properties->>'decimalLatitude' as FLOAT8)
            ), 4326) as _geom,
            st_transform(
                st_setsrid(
                    st_makepoint(
                    cast(ecatalogue.properties->>'decimalLongitude' as FLOAT8),
                    cast(ecatalogue.properties->>'decimalLatitude' as FLOAT8)
                    ),
                4326),
            3857) as _the_geom_webmercator
            FROM ecatalogue
            WHERE ecatalogue.properties->>'decimalLatitude' ~ '^[0-9\.]+$'
                AND cast(ecatalogue.properties->>'decimalLatitude' as FLOAT8) > -90
                AND cast(ecatalogue.properties->>'decimalLatitude' as FLOAT8) < 90
                AND ecatalogue.properties->>'decimalLongitude' ~ '^[0-9\.]+$'
                AND cast(ecatalogue.properties->>'decimalLongitude' as FLOAT8) >= -180
                AND cast(ecatalogue.properties->>'decimalLongitude' as FLOAT8) <= 180
            )
        """.format(table=self.table)
        connection.cursor().execute(query)
        connection.commit()

if __name__ == "__main__":
    luigi.run(main_task_cls=SpecimenDatasetTask)
