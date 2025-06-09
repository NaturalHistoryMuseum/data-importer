import click

from dataimporter.cli.utils import console, with_config
from dataimporter.importer import use_importer
from dataimporter.lib.config import Config


@click.group("view")
def view_group():
    pass


@view_group.command("list")
@with_config()
def list_views(config: Config):
    """
    List the name of all the views in current use.
    """
    with use_importer(config) as importer:
        console.log(", ".join(view.name for view in importer.views))


@view_group.command("flush")
@click.argument("view", type=str)
@with_config()
def flush(view: str, config: Config):
    """
    Flushes the given view's queue.

    For non-published views this should always be safe to do (but maybe think about it
    first, just to make sure) but for published views you should make sure you know what
    you're doing.
    """
    with use_importer(config) as importer:
        view = importer.get_view(view)
        console.log(
            f"Flushing {view.name} view queue, currently has", view.count(), "IDs in it"
        )
        view.flush()
        console.log(f"{view.name} flushed")


@view_group.command("ingest")
@click.argument("view", type=str)
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
    On the given view, updates MongoDB with any queued EMu changes and flushes its
    queue.
    """
    with use_importer(config) as importer:
        console.log(f"Adding changes from {view} view to mongo")
        importer.add_to_mongo(view, everything=everything)
        console.log(f"Added changes from {view} view to mongo")


@view_group.command("sync")
@click.argument("view", type=str)
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
    with use_importer(config) as importer:
        console.log(f"Syncing changes from {view} view to elasticsearch")
        importer.sync_to_elasticsearch(view, resync=resync)
        console.log(f"Finished with {view}")
