"""
Lookup command for premiumflow CLI.

Provides lookup functionality to find matching option transactions for a
position specification.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import List

import click
from rich.console import Console
from rich.table import Table

from ..core.parser import load_option_transactions, parse_lookup_input
from ..services.options import parse_option_description
from ..services.transactions import normalized_to_csv_dicts


def _build_results_table(position_spec: str, transactions: List[dict]) -> Table:
    """Create the Rich table used for lookup results."""
    table = Table(title=f"Position: {position_spec}")

    table.add_column("Date", style="cyan")
    table.add_column("Symbol", style="magenta")
    table.add_column("Code", style="green")
    table.add_column("Quantity", justify="right")
    table.add_column("Price", justify="right")
    table.add_column("Description", style="yellow")

    for txn in transactions:
        table.add_row(
            txn.get("Activity Date", ""),
            txn.get("Instrument", ""),
            txn.get("Trans Code", ""),
            txn.get("Quantity", ""),
            txn.get("Price", ""),
            txn.get("Description", ""),
        )

    return table


@click.command()
@click.argument("position_spec")
@click.option(
    "--file",
    "csv_file",
    type=click.Path(exists=True),
    default="all_transactions.csv",
    show_default=True,
    help="CSV file to search",
)
def lookup(position_spec, csv_file):
    """Look up a specific position in the CSV data."""
    console = Console()

    try:
        console.print(f"[blue]Looking up position: {position_spec}[/blue]")
        try:
            symbol, strike, option_type, expiration = parse_lookup_input(position_spec)
        except ValueError as exc:
            raise click.BadParameter(str(exc)) from exc

        parsed = load_option_transactions(
            csv_file,
            account_name=Path(csv_file).stem or "Lookup Account",
            account_number=f"{Path(csv_file).stem or 'Lookup Account'}-FILE",
        )
        transactions = normalized_to_csv_dicts(parsed.transactions)
        target_symbol = symbol.upper()
        target_option = "Call" if option_type.upper() == "C" else "Put"
        strike_decimal = Decimal(str(strike))
        expiration_parts = expiration.split("-")
        year_text, month_text, day_text = expiration_parts
        expiration_display = f"{int(month_text):02d}/{int(day_text):02d}/{year_text}"

        matches = []
        for txn in transactions:
            descriptor = parse_option_description(txn.get("Description", ""))
            if not descriptor:
                continue
            if descriptor.symbol != target_symbol:
                continue
            if descriptor.option_type != target_option:
                continue
            if descriptor.strike != strike_decimal:
                continue
            if descriptor.expiration != expiration_display:
                continue
            matches.append(txn)

        if matches:
            console.print(f"[green]Found {len(matches)} matching transactions[/green]")
            console.print(_build_results_table(position_spec, matches))
        else:
            console.print(f"[yellow]No transactions found for position: {position_spec}[/yellow]")

    except click.ClickException:
        raise
    except Exception as exc:
        console.print(f"[red]Error: {exc}[/red]")
        raise click.Abort() from exc
