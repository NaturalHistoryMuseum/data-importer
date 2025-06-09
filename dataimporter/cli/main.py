from datetime import datetime

import click
from rich.rule import Rule
from splitgill.manager import SearchVersion

from dataimporter.cli.emu import emu_group
from dataimporter.cli.ext import ext_group
from dataimporter.cli.maintenance import maintenance_group
from dataimporter.cli.portal import portal_group
from dataimporter.cli.utils import console, with_config
from dataimporter.cli.view import view_group
from dataimporter.importer import use_importer
from dataimporter.lib.config import Config


@click.group("dimp")
def cli():
    """
    The CLI for the data importer.
    """
    pass


@cli.command("status")
@with_config()
def get_status(config: Config):
    """
    Prints some status information about the importer's data.
    """
    with use_importer(config) as importer:
        console.log("Currently using config from", config.source)
        console.log("Latest EMu export queued:", importer.emu_status.get())
        console.log(Rule())
        console.log("Per view statistics:")
        console.log(Rule())

        not_published_views = [view for view in importer.views if not view.is_published]
        for view in sorted(not_published_views, key=lambda view: view.name):
            console.log("View:", view.name, f"({type(view).__name__})", style="bold")
            console.log("Backing store", view.store.name)
            console.log("Queue size:", view.count())
            console.log(Rule())

        published_views = [view for view in importer.views if view.is_published]
        for view in sorted(published_views, key=lambda view: view.name):
            console.log("View:", view.name, f"({type(view).__name__})", style="bold")
            console.log("Backing store", view.store.name)
            console.log("Queue size:", view.count())
            database = importer.get_database(view)
            console.log("Database:", database.name)
            console.log("MongoDB count:", database.data_collection.count_documents({}))
            console.log(
                "Elasticsearch latest count:",
                database.search(SearchVersion.latest).count(),
            )
            console.log(
                "Elasticsearch total count:",
                database.search(SearchVersion.all).count(),
            )
            m_version = database.get_committed_version()
            e_version = database.get_elasticsearch_version()
            console.log(
                "MongoDB version:",
                m_version,
                f"({datetime.fromtimestamp(m_version / 1000)})",
            )
            console.log(
                "Elasticsearch version:",
                e_version,
                f"({datetime.fromtimestamp(e_version / 1000)})",
            )
            if m_version is None and e_version is None:
                status = "No data"
            elif m_version is None:
                status = "No data in MongoDB but data in Elasticsearch. Something's up?"
            elif e_version is None:
                status = "No data in Elasticsearch, just need to sync"
            elif m_version == e_version:
                status = "In sync"
            elif m_version < e_version:
                status = "MongoDB is behind Elasticsearch. Something's up?"
            elif m_version > e_version:
                status = "Elasticsearch behind MongoDB, just need to sync"
            console.log("Status: ", status)
            console.log(Rule())


cli.add_command(emu_group)
cli.add_command(ext_group)
cli.add_command(view_group)
cli.add_command(portal_group)
cli.add_command(maintenance_group)

if __name__ == "__main__":
    cli()
