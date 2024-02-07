import code

import click

from dataimporter.cli.utils import with_config, console
from dataimporter.importer import DataImporter
from dataimporter.lib.config import Config


@click.group("dimp")
def cli():
    """
    The root CLI group for the dimp command.
    """
    pass


@cli.command()
@with_config()
def emu(config: Config):
    """
    Processes the available EMu exports, queuing, ingesting, and indexing one day's
    worth of data at a time ensuring each day's data is represented by a new version.
    """
    with DataImporter(config) as importer:
        while True:
            console.log("Queuing next dump set")
            dates_queued = importer.queue_emu_changes(only_one=True)

            # if no dates were queued, stop
            if not dates_queued:
                console.log("No more dumps to import, done")
                break
            else:
                console.log(
                    f"Dates queued: {', '.join(d.isoformat() for d in dates_queued)}"
                )

            # otherwise, add changes to mongo and elasticsearch for each view
            for name in ("specimen", "indexlot", "artefact", "mss", "preparation"):
                console.log(f"Adding changes from {name} view to mongo")
                importer.add_to_mongo(name)
                console.log(f"Syncing changes from {name} view to elasticsearch")
                importer.sync_to_elasticsearch(name, parallel=True)
                console.log(f"Finished with {name}")


@cli.command()
@with_config()
def gbif(config: Config):
    """
    Requests a new download of our specimen dataset from GBIF, downloads this DwC-A, and
    queues any changes found in it, then ingests and indexes any changes that cascade
    from these GBIF records to their associated specimen records.
    """
    with DataImporter(config) as importer:
        importer.queue_gbif_changes()
        importer.add_to_mongo("specimen")
        importer.sync_to_elasticsearch("specimen", parallel=True)


@cli.command()
@with_config()
def shell(config: Config):
    """
    Drops the caller into a Python shell with a DataImporter object (`importer`)
    instance available, thus allowing direct access to all methods.

    This is provided as purely a debugging tool.
    """
    with DataImporter(config) as importer:
        console.print("Starting shell...")
        env = {"importer": importer}
        code.interact(local=env)


if __name__ == "__main__":
    cli()
