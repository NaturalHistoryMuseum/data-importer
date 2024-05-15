import math

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
                importer.sync_to_elasticsearch(name)
                console.log(f"Finished with {name}")

            count += 1
            if count % 50 == 0:
                console.log(f"Count is {count}, forcing merges")
                for name in VIEW_NAMES:
                    console.log(f"Forcing merge on {name}")
                    importer.force_merge(name)
                    console.log(f"Forced merge on {name}")


@emu_group.command()
@click.argument("amount", type=str)
@with_config()
def queue(amount: str, config: Config):
    """
    Queues the specified amount of EMu exports.

    This can be "next", "all", or a positive number.
    """
    if amount == "next":
        amount = 1
        console.log(f"Queuing next dumpset only")
    elif amount == "all":
        amount = math.inf
        console.log(f"Queuing all available dumpsets")
    else:
        amount = int(amount)
        assert amount > 0, "Amount must be greater than 0"
        console.log(f"Queuing next {amount} dumpsets")

    with DataImporter(config) as importer:
        console.log(f"Current latest queued dumpset date: {importer.emu_status.get()}")

        while amount > 0:
            console.log("Queuing next dump set")
            dates_queued = importer.queue_emu_changes(only_one=True)
            if not dates_queued:
                console.log("No data to queue")
                break
            else:
                date_queued = dates_queued[0].isoformat()
                console.log(f"Date queued: {date_queued}")
            amount -= 1


@emu_group.command("update-mongo")
@click.argument("view", type=click.Choice(VIEW_NAMES))
@click.option(
    "--everything",
    is_flag=True,
    show_default=True,
    default=False,
    help="Add all records to MongoDB regardless of whether they have changed. This is "
    "primarily useful when a change has been made to a view's filters or "
    "representation",
)
@with_config()
def update_mongo(view: str, config: Config, everything: bool = False):
    """
    On the given view, updates MongoDB with any queued EMu changes and flushes the
    queues.
    """
    with DataImporter(config) as importer:
        console.log(f"Adding changes from {view} view to mongo")
        importer.add_to_mongo(view, everything=everything)
        console.log(f"Added changes from {view} view to mongo")
        console.log(f"Flushing queues")
        importer.flush_queues()
        console.log(f"Flushed queues")


@emu_group.command("update-es")
@click.argument("view", type=click.Choice(VIEW_NAMES))
@click.option(
    "--resync",
    is_flag=True,
    show_default=True,
    default=False,
    help="Resynchronise all data in Elasticsearch with MongoDB for this view, not just "
    "the records that have changed.",
)
@with_config()
def update_es(view: str, config: Config, resync: bool = False):
    """
    Updates Elasticsearch with the changes in MongoDB for the given view.
    """
    with DataImporter(config) as importer:
        console.log(f"Syncing changes from {view} view to elasticsearch")
        importer.sync_to_elasticsearch(view, resync=resync)
        console.log(f"Finished with {view}")
