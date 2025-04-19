Before the current version of the data importer, there were two previous versions which
worked in different ways.

## v2

Version 2, held in
[a private repository](https://github.com/NaturalHistoryMuseum/data-importer2), is very
similar to the current version but with a few key differences.

Records are read from the exports and entered _in their entirety_ into MongoDB.
This is where they are versioned.

After all the records are updated in MongoDB, a linking step runs where we follow the
relationship links between tables and add some additional version metadata to make
indexing faster.

Then there is then an indexing step which retrieves the records from MongoDB, transform
them into their presentation on the Portal, and adds them to ElasticSearch.

This whole process worked ok, but has a few downsides which are explained next and
inform why there is now a version 3 of this importer.

#### The EMu Data in MongoDB is Non-Standard

For all _non-EMu_ data in MongoDB, the index step simply takes the data out of MongoDB
and pushes it up to Elasticsearch.
No transformations are required, it's already in the right form.

For the _EMu data_, there are transformations required between MongoDB and
Elasticsearch.
This means that the EMu data has to be handled specially which is a bit weird.
If MongoDB is the base source for all the record data in the Data Portal, then why is
there non-base record data in there?
It's not right and makes things confusing.

#### There's So Much Data

Keeping the full data and diffs of every record in the EMu export is a really large
amount of data to store.
This causes performance issues that will only get worse over time.

#### The Code is Hard to Manage

A goal of the current data importer code is the complete _recreation_ of the _whole_
version history of the records on the Data Portal that come from the EMu exports _at any
time_.

This means the presentation (i.e. the transformation from the full EMu record to the
record seen on the Portal) of the records must stay constant at the time the version was
published which makes writing and maintaining the code to do this harder.
Bugs have to stay unless critical, for example, if there's a bug where a field was
mistyped and the data was published like that on the Portal, it has to stay in the code
even if we fix it in a newer version.
Changes to the presentation of the data have to be given a start and end version which
then must be released on a planned schedule, not just as and when.

Basically, it's a pain in the arse and I'm not sure why I convinced myself that this was
necessary when I originally implemented the new versioned backend.
There are benefits but the downsides are massive.

## v1

The first version of the importer is contained in the history of this repository.
It used PostgreSQL and Solr to store the data, along with a hack to CKAN's datastore
extension to make use of Solr for searches on the specimen/index lot/artefact resources.

This process was simple but pretty slow, and the transformation from PostgreSQL to Solr
was accomplished using an SQL select statement.
Again, this was simple and very manageable, but limited in terms of functionality.
