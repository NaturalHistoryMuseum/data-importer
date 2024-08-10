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
@click.option(
    "--delay-sync",
    is_flag=True,
    show_default=True,
    default=False,
    help="Perform one sync at the end instead of after each dump set",
)
def auto(config: Config, one: bool = False, delay_sync: bool = False):
    """
    Processes all the available EMu exports, queuing, ingesting, and indexing one day's
    worth of data at a time ensuring each day's data is represented by a new version.
    """
    with DataImporter(config) as importer:
        while True:
            console.log("Queuing next dump set")
            dates_queued = importer.queue_emu_changes(only_one=True)
            if not dates_queued:
                console.log("No more dumps to import, done")
                break

            console.log(f"Date queued: {dates_queued[0].isoformat()}")

            for name in VIEW_NAMES:
                console.log(f"Adding changes from {name} view to mongo")
                importer.add_to_mongo(name)
                console.log(f"Added changes from {name} view to mongo")

            if delay_sync:
                console.log(f"Delaying sync...")
            else:
                for name in VIEW_NAMES:
                    console.log(f"Syncing changes from {name} view to elasticsearch")
                    importer.sync_to_elasticsearch(name)
                    console.log(f"Finished with {name}")

            if one:
                console.log("Stopping after one day's data as requested")
                break

        if delay_sync:
            for name in VIEW_NAMES:
                console.log(f"Syncing changes from {name} view to elasticsearch")
                importer.sync_to_elasticsearch(name)
                console.log(f"Finished with {name}")


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


@emu_group.command("ingest")
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
def ingest(view: str, config: Config, everything: bool = False):
    """
    On the given view, updates MongoDB with any queued EMu changes and flushes the
    queues.
    """
    with DataImporter(config) as importer:
        console.log(f"Adding changes from {view} view to mongo")
        importer.add_to_mongo(view, everything=everything)
        console.log(f"Added changes from {view} view to mongo")


@emu_group.command("sync")
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
def sync(view: str, config: Config, resync: bool = False):
    """
    Updates Elasticsearch with the changes in MongoDB for the given view.
    """
    with DataImporter(config) as importer:
        console.log(f"Syncing changes from {view} view to elasticsearch")
        importer.sync_to_elasticsearch(view, resync=resync)
        console.log(f"Finished with {view}")


@emu_group.command("get-emu-date")
@click.argument("amount", type=str)
@with_config()
def get_emu_date(config: Config):
    """
    Prints the latest date we've imported EMu exports for.
    """
    with DataImporter(config) as importer:
        console.log(importer.emu_status.get())
