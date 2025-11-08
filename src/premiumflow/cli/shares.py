"""
Inspect persisted stock lots via the CLI.

This command provides a user-facing view of consolidated stock lots sourced from the
SQLite persistence layer. Output can be rendered as a rich table or JSON payload.
"""

from __future__ import annotations

import json
from typing import Optional

import click
from rich.console import Console
from rich.table import Table

from ..persistence import SQLiteRepository
from ..services.cli_helpers import format_account_label
from ..services.display import format_currency
from ..services.stock_lots import (
    StockLotSummary,
    fetch_stock_lot_summaries,
    serialize_stock_lot_summary,
)

StatusChoice = click.Choice(["all", "open", "closed"], case_sensitive=False)
FormatChoice = click.Choice(["table", "json"], case_sensitive=False)


@click.command("shares")
@click.option("--account-name", help="Filter lots by account name.")
@click.option("--account-number", help="Filter lots by account number.")
@click.option("--ticker", help="Filter lots by ticker symbol.")
@click.option(
    "--status",
    type=StatusChoice,
    default="all",
    show_default=True,
    help="Filter lots by status (open, closed, or all).",
)
@click.option(
    "--format",
    "output_format",
    type=FormatChoice,
    default="table",
    show_default=True,
    help="Output format.",
)
def shares(
    account_name: Optional[str],
    account_number: Optional[str],
    ticker: Optional[str],
    status: str,
    output_format: str,
) -> None:
    """Display persisted stock lots with optional filtering."""

    repository = SQLiteRepository()
    summaries = fetch_stock_lot_summaries(
        repository,
        account_name=account_name,
        account_number=account_number,
        ticker=ticker,
        status=status.lower(),  # type: ignore[arg-type]
    )

    if output_format.lower() == "json":
        payload = {"lots": [serialize_stock_lot_summary(summary) for summary in summaries]}
        click.echo(json.dumps(payload, indent=2))
        return

    console = Console(width=200, force_terminal=False)
    if not summaries:
        console.print("[yellow]No stock lots match the requested filters.[/yellow]")
        return

    table = _build_stock_lot_table(summaries)
    console.print(table)


def _build_stock_lot_table(summaries: list[StockLotSummary]) -> Table:
    table = Table(title="Stock Lots", expand=True)
    table.add_column("Account", style="cyan", no_wrap=True)
    table.add_column("Symbol", style="magenta", no_wrap=True)
    table.add_column("Direction", style="yellow", no_wrap=True)
    table.add_column("Status", style="yellow", no_wrap=True)
    table.add_column("Opened", style="cyan", no_wrap=True)
    table.add_column("Closed", style="cyan", no_wrap=True)
    table.add_column("Shares", justify="right")
    table.add_column("Basis/Share", justify="right")
    table.add_column("Basis Total", justify="right")
    table.add_column("Realized P&L", justify="right")
    table.add_column("Assignment", style="magenta", no_wrap=True)

    for summary in summaries:
        account_label = format_account_label(summary.account_name, summary.account_number)
        table.add_row(
            account_label,
            summary.symbol,
            summary.direction.upper(),
            summary.status.upper(),
            summary.opened_at,
            summary.closed_at or "--",
            f"{abs(summary.quantity)}",
            format_currency(summary.basis_per_share),
            format_currency(summary.basis_total),
            format_currency(summary.realized_pnl_total),
            summary.assignment_kind or "--",
        )

    return table
