/*
migration: 20180530-1
database: datastore_default
description: adding new pending column to emultimedia and setting all existing
             values to false.
*/

-- add the new boolean column
alter table emultimedia add column pending boolean;
-- set the default value to false as all entries prior to this migration's run
-- will not be pending. Setting the value on all existing rows like this is
-- better than using a default value when altering the table as we avoid locking
-- the entire table whilst updating some 2million rows which will take a while.
update emultimedia set pending = false;


-- drop the materialized view _multimedia_view so that it can be redefined
drop materialized view _multimedia_view;
-- redefine the materialized view
CREATE MATERIALIZED VIEW _multimedia_view AS (
    SELECT
        _ecatalogue__emultimedia.irn,
        COALESCE(jsonb_agg(
            jsonb_build_object('identifier', format('http://www.nhm.ac.uk/services/media-store/asset/%s/contents/preview', emultimedia.properties->>'assetID'),
            'type', 'StillImage',
            'license',  'http://creativecommons.org/licenses/by/4.0/',
            'rightsHolder',  'The Trustees of the Natural History Museum, London') || emultimedia.properties)
            FILTER (WHERE emultimedia.irn IS NOT NULL), NULL)::TEXT as multimedia,
        string_agg(DISTINCT emultimedia.properties->>'category', ';') as category
    FROM emultimedia
    INNER JOIN _ecatalogue__emultimedia ON _ecatalogue__emultimedia.rel_irn = emultimedia.irn
    WHERE
        (embargo_date IS NULL OR embargo_date < NOW())
      AND deleted IS NULL
      AND pending IS FALSE
    GROUP BY _ecatalogue__emultimedia.irn);

CREATE UNIQUE INDEX ON _multimedia_view (irn);
