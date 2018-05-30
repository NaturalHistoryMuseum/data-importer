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
