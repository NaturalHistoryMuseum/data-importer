ke2sql
=======

Import KE EMu data into SQL. 


/usr/lib/ke2sql/bin/python commands/run_one.py --local-scheduler






copy (select "gbifID" AS "ID","gbifOccurrenceID" AS "occurrenceID", "gbifLastInterpreted" AS "lastInterpreted","gbifLastParsed" AS "lastParsed","gbifIssue" AS "issue","gbifKingdom" AS "kingdom","gbifKingdomKey" AS "kingdomKey","gbifPhylum" AS "phylum","gbifPhylumKey" AS "phylumKey","gbifClass" AS "class","gbifClassKey" AS "classKey","gbifOrder" AS "order","gbifOrderKey" AS "orderKey","gbifFamily" AS "family","gbifFamilyKey" AS "familyKey","gbifGenus" AS "genus","gbifGenusKey" AS "genusKey","gbifSubgenus" AS "subGenus","gbifSubgenusKey" AS "subGenusKey","gbifSpecies" AS "species","gbifSpeciesKey" AS "speciesKey","gbifTaxonRank" AS "taxonRank","gbifIdentifiedBy" AS "identifiedBy","gbifScientificName" AS "scientificName","gbifTaxonKey" AS "taxonKey","gbifRecordedBy" AS "recordedBy","gbifEventDate" AS "eventDate","gbifRecordNumber" AS "recordNumber","gbifContinent" AS "continent","gbifCountry" AS "country","gbifStateProvince" AS "stateProvince","gbifHabitat" AS "habitat","gbifCountryCode" AS "countryCode","gbifIslandGroup" AS "islandGroup","gbifDecimalLongitude" AS "decimalLongitude","gbifDecimalLatitude" AS "decimalLatitude" from gbif.occurrence)  to '/usr/lib/ke2sql/src/data-importer/data_importer/data/gbif.csv' with CSV HEADER;