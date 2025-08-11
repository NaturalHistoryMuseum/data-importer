## v1.2.1 (2025-08-11)

### Docs

- fix line breaks in readme
- update docs config

### Style

- auto format python

### CI System(s)

- update github workflows

### Chores/Misc

- fix file endings
- add PR templates, contributing guidelines, etc
- update tool configs

## v1.2.0 (2025-07-08)

### Feature

- add controlled locking to ensure one importer is running at once

### Fix

- correct palaeontology typo

### CI System(s)

- add bump workflow for automatic releases

### Chores/Misc

- correct project version

## v1.1.0 (2025-05-16)

### Fix

- update splitgill with bug fixes

## v1.0.1 (2025-04-20)

### Feature

- add dimp as a cli script

### Chores/Misc

- update version to 1.0.1

## v1.0.0 (2025-04-19)

### Breaking Changes

- rewrite links, move embargo and redaction management down to store, change many things
- Upgrade to elasticsearch 8
- remove all old code

### Feature

- update version number
- upgrade to stable splitgill
- update splitgill version
- remove image orientation field, apply it instead
- update to latest splitgill version
- allow bulk options indexing to be specified in config
- update to newest splitgill version
- rename width and height image fields to align with AC
- add exif orientation tag value to mss docs
- add occurrenceID to preparation
- rename two preparation fields after refining mapping
- add sg prefix
- add purpose field to preparations
- allow specifying the mongodb database name
- add a new view CLI command for managing views
- add a status function to the CLI
- add a source parameter to Config object
- rewrite links, move embargo and redaction management down to store, change many things
- switch DarCollector to CollEventFullNameSummaryData for specimen recordedBy
- filter out really not needed EMu fields
- include media on prep records
- add on loan filtering to preps view
- remove killing agent from strings
- add iter_all function to views for convenient all record iteration
- add gbif shortcut method to shell
- add a banner to the interactive shell listing the available vars
- add more functions to the maintenance shell
- add control over how much data is added/synced
- add iter_and_delete method to ChangeQueue db
- introduce more cli functions and reorganise
- add view names to utils for cli functions
- add a util function to get an API key from Portal database
- reduce chunk size for bulk indexing
- add force merge method to importer
- provide modified field to avoid unnecessary adds
- return committed version from add_to_mongo method (or None)
- add flush queues method to importer
- add a portal config section to the config
- add a basic cli
- return the dates that have been queued from the queue_emu_changes method
- provide default config options
- add mammal part preps
- add a download object wrapper to stash more properties of the download
- wrap emu dumps into dumpsets to make them easier to manage
- perform a delete in leveldb when deleting a record
- add a new option allowing the processing of EMu dumps to halt after 1 date's worth has been processed
- add a new util function that combines lines of text back into a single string
- add 3d view for emultimedia records
- add preparations view
- add preparation_id config option
- add a way to redact records
- add redaction capabilities to dbs
- Upgrade to elasticsearch 8
- add a reduce parameter to get_all_values
- don't publish individual counts <= 0
- use the new IIIF base URL config value in the Image view
- add a iiif base URL config option
- add DataImporter controller class
- add link code for media, taxonomy, and gbif
- add GBIFView and code to download GBIF data
- add EMu based views
- add View and ViewLink classes
- add a config module with tests
- remove converters, remove audit specific dump class, alter embargo reading code
- remove versioning from source records handled by this lib + a few other bits
- create EMu export reading models and logic

### Fix

- remove new mammal parts field from specimen
- remove sample/preps view from portal registration cli
- change config name for worker count
- ensure log date format is correct
- do not ignore empty exif rows
- add old_asset_id back into mss view
- only use a list for specimen preparations if there is more than 1
- use the correct property to check if the view has a database
- check view's database existence correctly in status CLI function
- use r-string for regex in killing agent cleaner
- fix on loan filter to actually work on summary data
- stop when we can't find a record in the print_record_data function
- remove parallel parameter (no longer used)
- use correct wildcard index name access
- organise and add missing imports
- queue new releases before adding to mongo
- return an empty list if no emu changes queued
- remove reference to uncommitted code
- stop resetting the embargo queue on flush
- combine PalArtDescription values into several lines of text rather than an array
- correct typo in viceCountry field
- ensure media is order by integer ID not str ID
- correct the base view on the specimen TaxonomyLink
- check for valid GUID on mss records
- check for a valid GUID on images too
- fix essentially a buffer overload bug in the DataDB
- parse out person strings from certain specimen fields
- remove registeredWeight (redaction)
- rename gbifIssues field gbifIssue to match existing implementation

### Refactor

- remove is_mammal_part function which was only used in one place
- reorder preparation data
- rename view.sg_nameto view.pulished_name and use "published" for views with SG DBs
- use exceptions when failing to get stores, views, and databases
- rename get_splitgill_database importer method get_database and make more user friendly
- move VIEW_NAMES to emu cli module as they are EMu only
- remove unused cli arg (woops)
- change queue_emu_changes to always do one day's worth of data
- move the ChangeQueue down
- rename preps to sample collection on Portal
- replace cytoolz's partition_all with splitgill's partition
- change the is_web_published check to be more explicit
- use centrally defined field name for EMu GUIDs
- move ref field definitions for taxonomy links to the TaxonomyLink class
- fix an incorrectly named test
- abstract out ManyToManyViewLink from MediaLink
- remove unecessary redeclaration of abstractmethod
- rename ManyToOneLink to ManyToOneViewLink
- move all framework modules into a lib dir
- move the ManyToOneLink class into the view module
- abstract out common many-to-one link types into an abc and clarify Index db usage
- refactor dumps helpers to one file
- move EMuRecord out to a non-emu module and rename

### Docs

- update docker compose test run command
- add comment about mss naming
- fix on loan logic in diagram
- add documentation about the EMu views
- remove todo
- add doc for release_records method
- add a todo for later
- remove a couple more todos
- remove never going to be implemented todo
- correct wildly incorrect docstring
- add a comment to explain the determination* fields use of clean=False
- add todo about aligning with AC (Audiovisual Core, used to be called Audobon Core until quite recently)
- remove unecessary todo
- add a comment to explain the DEPARTMENT_COLLECTION_CODES dict
- add docs and setup a few bits and bobs

### Style

- reformat test file
- reorder imports
- remove an unused import

### Tests

- fix tests after removing mammal part/parent link data
- allow mongo and es connection details to be specified via envvars
- fix test after field rename
- fix the tests after the most recent changes
- fix test after store exception changes
- fix docker tests by using correct names
- add test scenarios for on loan filter
- simplify tests by using splitgill utils
- fix config tests after adding portal config section
- add tests for recent prep view changes
- add comments to the dumps helper module's functions
- move dump reading helper to dumps module
- ensure the dump path exists before creating a mock dump
- add tests for gbif related code
- add tests for the new views

### Build System(s)

- remove single-quote ruff formatting rule
- add yaml to dependencies
- switch es config to env var
- update dependencies (versions and remove unused)
- remove cytoolz from dependencies
- make sure pyproject.toml is up to date
- Add default config for docker and modifications needed to use it in docker
- add missed msgpack dependency
- add github action stuff

### CI System(s)

- update precommit
- remove pytest -x parameter to allow all tests to run
- force tests to stop after first failure so that we can see what's wrong
- add -v flag to see why github actions tests are getting stuck
- fix test action and update checkout version

### Chores/Misc

- remove field name question todo
- add date to cli logs
- better represent the options as not the defaults, they just are the options
- rename preparations dataset/resource to samples
- add auth check to gbif cli command
- print info about published and unpublished views separately for ease of looking at it
- remove unused DataImporter method, flush_queues
- use new has_database property when creating a SplitgillDatabase
- add a more convenient way of checking if a view feeds a database
- remove prd shell function, don't need it
- remove unused function and test
- do some minor fiddling with emu cli
- remove checked todo
- remove random comments from 3d view
- remove old dumps not being used anymore
- switch a number of fields to get_first instead of get_all
- change a few get_alls to get_firsts where there is only ever 1 value
- switch from deprecated fast_int to try_int and other minor comments/refactors
- make the image "mime" field actually a mime type
- switch taxonomy view to only use get_first instead of get_all
- remove unused variable
- in the artefact view, switch get_alls to get_firsts
- make the dataimporter.ext folder a module
- remove all old code
