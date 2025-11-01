"""
Command-line interface for premiumflow.

Provides the CLI command group and registers individual subcommands.
"""

from __future__ import annotations

import click

from .analyze import analyze
from .import_command import import_group
from .lookup import lookup
from .trace import trace


@click.group()
@click.version_option(version="0.1.0")
def main():
    """PremiumFlow - Options trading roll chain analysis tool."""
    pass


# Register CLI subcommands
main.add_command(analyze)
main.add_command(import_group)
main.add_command(lookup)
main.add_command(trace)


if __name__ == "__main__":
    main()
