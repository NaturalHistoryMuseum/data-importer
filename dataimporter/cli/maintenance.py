import click
import code

from dataimporter.cli.utils import with_config, console, VIEW_NAMES
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
        for name in VIEW_NAMES:
            console.log(f"Force merge on {name} indices")
            importer.force_merge(name)
            console.log(f"{name} complete")


@maintenance_group.command("update-profiles")
@with_config()
@click.option(
    "--rebuild",
    is_flag=True,
    show_default=True,
    default=False,
    help="Rebuild all profiles for each database",
)
def update_profiles(config: Config, rebuild: bool = False):
    """
    Updates the profile data for each database.
    """
    with DataImporter(config) as importer:
        for name in VIEW_NAMES:
            sg_db = importer.sg_dbs[name]
            console.log(f"Updating profiles for {name} database")
            sg_db.update_profiles(rebuild=rebuild)
            console.log(f"Completed updating profiles for {name} database")


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
        env = {"importer": importer}
        code.interact(local=env)
