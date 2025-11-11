"""CLI command for displaying combined equity and option positions."""

from __future__ import annotations

import json
from dataclasses import asdict
from decimal import Decimal
from typing import Iterable, Optional

import click
from rich.console import Console
from rich.table import Table

from ..persistence import SQLiteRepository
from ..services.display import format_currency
from ..services.positions import (
    EquityPosition,
    OptionPosition,
    fetch_positions,
)

FormatChoice = click.Choice(["table", "json"], case_sensitive=False)


def _decimal_to_string(value: Decimal) -> str:
    return format(value, "f")


def _serialize_equity(position: EquityPosition) -> dict[str, object]:
    data = asdict(position)
    data["basis_total"] = _decimal_to_string(position.basis_total)
    data["basis_per_share"] = _decimal_to_string(position.basis_per_share)
    data["realized_pnl_total"] = _decimal_to_string(position.realized_pnl_total)
    return data


def _serialize_option(position: OptionPosition) -> dict[str, object]:
    data = asdict(position)
    data["strike"] = _decimal_to_string(position.strike)
    data["open_credit"] = _decimal_to_string(position.open_credit)
    data["open_fees"] = _decimal_to_string(position.open_fees)
    data["credit_remaining"] = _decimal_to_string(position.credit_remaining)
    return data


def _build_equity_table(rows: Iterable[EquityPosition]) -> Table:
    table = Table(title="Equity Positions", expand=True)
    table.add_column("Account", style="cyan", no_wrap=True)
    table.add_column("Symbol", style="magenta", no_wrap=True)
    table.add_column("Direction", style="yellow", no_wrap=True)
    table.add_column("Shares", justify="right")
    table.add_column("Basis/Share", justify="right")
    table.add_column("Basis Total", justify="right")
    table.add_column("Realized P&L", justify="right")

    for position in rows:
        account_number = f" ({position.account_number})" if position.account_number else ""
        table.add_row(
            f"{position.account_name}{account_number}",
            position.symbol,
            position.direction.upper(),
            f"{position.shares}",
            format_currency(position.basis_per_share),
            format_currency(position.basis_total),
            format_currency(position.realized_pnl_total),
        )
    return table


def _build_option_table(rows: Iterable[OptionPosition]) -> Table:
    table = Table(title="Option Positions", expand=True)
    table.add_column("Account", style="cyan", no_wrap=True)
    table.add_column("Symbol", style="magenta", no_wrap=True)
    table.add_column("Type", style="magenta", no_wrap=True)
    table.add_column("Strike", justify="right")
    table.add_column("Expiration", style="cyan", no_wrap=True)
    table.add_column("Direction", style="yellow", no_wrap=True)
    table.add_column("Contracts", justify="right")
    table.add_column("Open Credit", justify="right")
    table.add_column("Open Fees", justify="right")
    table.add_column("Credit Remaining", justify="right")

    for position in rows:
        account_number = f" ({position.account_number})" if position.account_number else ""
        table.add_row(
            f"{position.account_name}{account_number}",
            position.symbol,
            position.option_type.upper(),
            format_currency(position.strike),
            position.expiration,
            position.direction.upper(),
            f"{position.contracts}",
            format_currency(position.open_credit),
            format_currency(position.open_fees),
            format_currency(position.credit_remaining),
        )
    return table


@click.command("positions")
@click.option("--account-name", help="Filter positions by account name.")
@click.option("--account-number", help="Filter positions by account number.")
@click.option("--ticker", help="Filter positions by ticker symbol.")
@click.option(
    "--format",
    "output_format",
    type=FormatChoice,
    default="table",
    show_default=True,
    help="Output format.",
)
def positions_command(
    account_name: Optional[str],
    account_number: Optional[str],
    ticker: Optional[str],
    output_format: str,
) -> None:
    """Display combined equity and option positions."""

    repository = SQLiteRepository()
    equities, options = fetch_positions(
        repository,
        account_name=account_name,
        account_number=account_number,
        ticker=ticker,
    )

    if output_format.lower() == "json":
        payload = {
            "equities": [_serialize_equity(pos) for pos in equities],
            "options": [_serialize_option(pos) for pos in options],
        }
        click.echo(json.dumps(payload, indent=2))
        return

    console = Console(width=200, force_terminal=False)
    if not equities and not options:
        console.print("[yellow]No positions match the requested filters.[/yellow]")
        return

    if equities:
        console.print(_build_equity_table(equities))
    if options:
        console.print()
        console.print(_build_option_table(options))
