import click
import math

from dataimporter.cli.utils import with_config, console
from dataimporter.importer import DataImporter
from dataimporter.lib.config import Config


EMU_VIEWS = ("specimen", "indexlot", "artefact", "mss", "preparation")


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
            date_queued = importer.queue_emu_changes()
            if date_queued is None:
                console.log("No more dumps to import, done")
                break

            console.log(f"Date queued: {date_queued.isoformat()}")

            for name in EMU_VIEWS:
                console.log(f"Adding changes from {name} view to mongo")
                importer.add_to_mongo(name)
                console.log(f"Added changes from {name} view to mongo")

            if delay_sync:
                console.log(f"Delaying sync...")
            else:
                for name in EMU_VIEWS:
                    console.log(f"Syncing changes from {name} view to elasticsearch")
                    importer.sync_to_elasticsearch(name)
                    console.log(f"Finished with {name}")

            if one:
                console.log("Stopping after one day's data as requested")
                break

        if delay_sync:
            for name in EMU_VIEWS:
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
            date_queued = importer.queue_emu_changes()
            if date_queued is None:
                console.log("No data to queue")
                break
            else:
                console.log(f"Date queued: {date_queued.isoformat()}")
            amount -= 1


@emu_group.command("get-emu-date")
@with_config()
def get_emu_date(config: Config):
    """
    Prints the latest date we've imported EMu exports for.
    """
    with DataImporter(config) as importer:
        console.log(importer.emu_status.get())
