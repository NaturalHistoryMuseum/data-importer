import click

from dataimporter.cli.emu import emu_group
from dataimporter.cli.ext import ext_group
from dataimporter.cli.maintenance import maintenance_group
from dataimporter.cli.portal import portal_group


@click.group("dimp")
def cli():
    """
    The CLI for the data importer.
    """
    pass


cli.add_command(emu_group)
cli.add_command(ext_group)
cli.add_command(portal_group)
cli.add_command(maintenance_group)

if __name__ == "__main__":
    cli()
