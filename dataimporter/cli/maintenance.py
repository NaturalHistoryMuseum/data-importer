import code

import click

from dataimporter.cli.shell import setup_env
from dataimporter.cli.utils import with_config, console
from dataimporter.importer import DataImporter
from dataimporter.lib.config import Config


@click.group("maintenance")
def maintenance_group():
    pass


@maintenance_group.command("merge")
@with_config()
def merge(config: Config):
    """
    Perform a force merge on each view's indices.

    This cleans up deleted documents etc.
    """
    with DataImporter(config) as importer:
        for view in importer.views:
            if view.has_database:
                console.log(f"Force merge on {view.name} indices")
                importer.force_merge(view.name)
                console.log(f"{view.name} complete")


@maintenance_group.command()
@with_config()
def shell(config: Config):
    """
    Drops the caller into a Python shell with a DataImporter object (`importer`)
    instance available, thus allowing direct access to all methods.

    This is provided as purely a debugging tool, use at your own risk!
    """
    with DataImporter(config) as importer:
        console.print("Starting shell...")
        env = setup_env(importer)
        banner = f"Available variables/functions: {', '.join(env.keys())}"
        code.interact(banner=banner, local=env)
