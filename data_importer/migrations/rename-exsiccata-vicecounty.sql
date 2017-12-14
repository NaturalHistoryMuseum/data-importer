/*
migration: 20171214-1
database: datastore_default
description: renaming the keys exsiccati, exsiccatiNumber, and viceCountry.
*/

update ecatalogue
set
  properties = properties - 'exsiccatiNumber' - 'exsiccati' - 'viceCountry'
    ||
    jsonb_build_object('exsiccataNumber', properties -> 'exsiccatiNumber', 'exsiccata',
                       properties -> 'exsiccati', 'viceCounty', properties -> 'viceCountry');

-- should be 0 if successful
select count(key) as "failed changes:"
from (select jsonb_object_keys(properties) as key
      from ecatalogue) as keys
where
  key in ('exsiccati', 'exsiccatiNumber', 'viceCountry');