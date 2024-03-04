import click

from dataimporter.cli.utils import with_config, console, VIEW_NAMES
from dataimporter.importer import DataImporter
from dataimporter.lib.config import Config


@click.group("emu")
def emu_group():
    pass


@emu_group.command()
@with_config()
@click.option(
    "--one",
    is_flag=True,
    show_default=True,
    default=False,
    help="Only process the next available day's worth of EMu exports",
)
def auto(config: Config, one: bool = False):
    """
    Processes all the available EMu exports, queuing, ingesting, and indexing one day's
    worth of data at a time ensuring each day's data is represented by a new version.
    """
    count = 0

    with DataImporter(config) as importer:
        while True:
            console.log("Queuing next dump set")
            dates_queued = importer.queue_emu_changes(only_one=True)

            if not dates_queued:
                console.log("No more dumps to import, done")
                break

            date_queued = dates_queued[0].isoformat()
            console.log(f"Date queued: {date_queued}")

            if one:
                console.log("Stopping after one day's data as requested")
                break

            for name in VIEW_NAMES:
                console.log(f"Adding changes from {name} view to mongo")
                importer.add_to_mongo(name)
                console.log(f"Added changes from {name} view to mongo")

            console.log(f"Flushing queues")
            importer.flush_queues()
            console.log(f"Flushed queues")

            for name in VIEW_NAMES:
                console.log(f"Syncing changes from {name} view to elasticsearch")
                importer.sync_to_elasticsearch(name, parallel=True)
                console.log(f"Finished with {name}")

            count += 1
            if count % 50 == 0:
                console.log(f"Count is {count}, forcing merges")
                for name in VIEW_NAMES:
                    console.log(f"Forcing merge on {name}")
                    importer.force_merge(name)
                    console.log(f"Forced merge on {name}")


@emu_group.command("queue-next")
@with_config()
def queue_next(config: Config):
    """
    Queues the next day's data and does nothing else.
    """
    with DataImporter(config) as importer:
        dates_queued = importer.queue_emu_changes(only_one=True)
        if not dates_queued:
            console.log("No data to queue")
        else:
            date_queued = dates_queued[0].isoformat()
            console.log(f"Date queued: {date_queued}")


@emu_group.command("update-mongo")
@with_config()
def update_mongo(config: Config):
    """
    Updates MongoDB with any queued EMu changes and flushes the queues.
    """
    with DataImporter(config) as importer:
        for name in VIEW_NAMES:
            console.log(f"Adding changes from {name} view to mongo")
            importer.add_to_mongo(name)
            console.log(f"Added changes from {name} view to mongo")

        console.log(f"Flushing queues")
        importer.flush_queues()
        console.log(f"Flushed queues")


@emu_group.command("update-es")
@with_config()
def update_es(config: Config):
    """
    Updates Elasticsearch with the changes in MongoDB.
    """
    with DataImporter(config) as importer:
        for name in VIEW_NAMES:
            console.log(f"Syncing changes from {name} view to elasticsearch")
            importer.sync_to_elasticsearch(name, parallel=True)
            console.log(f"Finished with {name}")
