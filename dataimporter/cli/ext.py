import click

from dataimporter.cli.utils import console
from dataimporter.importer import DataImporter
from dataimporter.lib.config import Config


@click.group("ext")
def ext_group():
    pass


@ext_group.command()
def gbif(config: Config):
    """
    Requests a new download of our specimen dataset from GBIF, downloads this DwC-A, and
    queues any changes found in it, then ingests and indexes any changes that cascade
    from these GBIF records to their associated specimen records.
    """
    with DataImporter(config) as importer:
        console.log("Queuing new GBIF changes")
        importer.queue_gbif_changes()
        console.log("Updating specimen data in MongoDB")
        importer.add_to_mongo("specimen")
        console.log("Syncing specimen changes to Elasticsearch")
        importer.sync_to_elasticsearch("specimen", parallel=True)
        console.log("Done")
