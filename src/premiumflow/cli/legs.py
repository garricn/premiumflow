"""
Legs command for premiumflow CLI.

Provides the CLI entry point that displays matched option legs with FIFO matching.
"""

from __future__ import annotations

from datetime import date
from typing import Dict, Optional

import click
from rich.console import Console
from rich.table import Table

from ..persistence import SQLiteRepository
from ..services.display import format_currency
from ..services.leg_matching import (
    MatchedLeg,
    _stored_to_normalized,
    group_fills_by_account,
    match_legs_with_errors,
)

# Type alias for leg dictionary keys
LegKey = tuple[str, Optional[str], str]  # (account_name, account_number, leg_id)


def _parse_date(date_str: str) -> date:
    """Parse a date string in YYYY-MM-DD format."""
    return date.fromisoformat(date_str)


def _format_resolution(resolution: Optional[str]) -> str:
    """Format resolution code for display."""
    if resolution is None:
        return "—"
    return resolution


def _format_date(d: Optional[date]) -> str:
    """Format date for display."""
    if d is None:
        return "—"
    return d.isoformat()


def _build_legs_table(legs_dict: Dict[LegKey, MatchedLeg], *, show_lots: bool = False) -> Table:
    """Build a Rich table displaying matched legs."""
    if show_lots:
        table = Table(title="Matched Legs with Lot Details")
        table.add_column("Symbol", style="cyan")
        table.add_column("Expiration", style="magenta")
        table.add_column("Strike", justify="right")
        table.add_column("Type", style="yellow")
        table.add_column("Lot", justify="right")
        table.add_column("Status", style="green")
        table.add_column("Qty", justify="right")
        table.add_column("Opened", style="blue")
        table.add_column("Closed", style="blue")
        table.add_column("Open Credit", justify="right")
        table.add_column("Close Cost", justify="right")
        table.add_column("Realized", justify="right")
        table.add_column("Fees", justify="right")
    else:
        table = Table(title="Matched Legs")
        table.add_column("Symbol", style="cyan")
        table.add_column("Expiration", style="magenta")
        table.add_column("Strike", justify="right")
        table.add_column("Type", style="yellow")
        table.add_column("Opened", style="blue")
        table.add_column("Closed", style="blue")
        table.add_column("Resolution", style="green")
        table.add_column("Open Qty", justify="right")
        table.add_column("Net Contracts", justify="right")
        table.add_column("Open Credit", justify="right")
        table.add_column("Close Cost", justify="right")
        table.add_column("Realized", justify="right")
        table.add_column("Fees", justify="right")

    for leg in sorted(
        legs_dict.values(),
        key=lambda leg_item: (
            leg_item.contract.symbol,
            leg_item.contract.expiration,
            leg_item.contract.strike,
        ),
    ):
        if show_lots:
            # Show one row per lot
            for lot_idx, lot in enumerate(leg.lots, start=1):
                table.add_row(
                    leg.contract.symbol,
                    leg.contract.expiration.isoformat(),
                    str(leg.contract.strike),
                    leg.contract.option_type,
                    str(lot_idx),
                    lot.status,
                    str(lot.quantity),
                    _format_date(lot.opened_at),
                    _format_date(lot.closed_at),
                    format_currency(lot.open_credit_gross),
                    format_currency(lot.close_cost),
                    format_currency(lot.realized_premium),
                    format_currency(lot.total_fees),
                )
        else:
            # Show summary row per leg
            table.add_row(
                leg.contract.symbol,
                leg.contract.expiration.isoformat(),
                str(leg.contract.strike),
                leg.contract.option_type,
                _format_date(leg.opened_at),
                _format_date(leg.closed_at),
                _format_resolution(leg.resolution()),
                str(leg.open_quantity),
                str(leg.net_contracts),
                format_currency(leg.open_credit_gross),
                format_currency(leg.close_cost),
                format_currency(leg.realized_premium),
                format_currency(leg.total_fees),
            )

    return table


@click.command()
@click.option(
    "--account-name",
    help="Filter by account name",
)
@click.option(
    "--account-number",
    help="Filter by account number",
)
@click.option(
    "--ticker",
    help="Filter by ticker symbol",
)
@click.option(
    "--since",
    help="Filter transactions since date (YYYY-MM-DD)",
)
@click.option(
    "--until",
    help="Filter transactions until date (YYYY-MM-DD)",
)
@click.option(
    "--status",
    type=click.Choice(["all", "open", "closed"], case_sensitive=False),
    default="all",
    help="Filter by leg status (default: all)",
)
@click.option(
    "--lots",
    "show_lots",
    is_flag=True,
    help="Show detailed lot information",
)
def legs(account_name, account_number, ticker, since, until, status, show_lots):
    """Display matched option legs with FIFO matching."""
    console = Console()

    # Parse dates if provided
    since_date = None
    if since:
        try:
            since_date = _parse_date(since)
        except ValueError as exc:
            raise click.BadParameter(f"Invalid date format: {since}. Use YYYY-MM-DD") from exc

    until_date = None
    if until:
        try:
            until_date = _parse_date(until)
        except ValueError as exc:
            raise click.BadParameter(f"Invalid date format: {until}. Use YYYY-MM-DD") from exc

    try:
        # Fetch transactions from repository
        repo = SQLiteRepository()
        stored_txns = repo.fetch_transactions(
            account_name=account_name,
            account_number=account_number,
            ticker=ticker,
            since=since_date,
            until=until_date,
            status="all",  # Repository status is for imports, not legs
        )

        if not stored_txns:
            console.print("[yellow]No transactions found matching the specified filters.[/yellow]")
            return

        # Convert stored transactions to normalized
        normalized_txns = [_stored_to_normalized(stored) for stored in stored_txns]

        # Group by account and convert to fills
        all_fills = group_fills_by_account(normalized_txns)

        # Match legs with error handling
        matched, errors = match_legs_with_errors(all_fills)

        # Filter by leg status if requested
        if status == "open":
            matched = {k: v for k, v in matched.items() if v.is_open}
        elif status == "closed":
            matched = {k: v for k, v in matched.items() if not v.is_open}

        # Display errors if any
        if errors:
            console.print("[yellow]Matching Errors:[/yellow]")
            for error in errors:
                console.print(f"  [red]{error}[/red]")
            console.print()

        if not matched:
            console.print("[yellow]No legs found matching the specified filters.[/yellow]")
            return

        # Display legs table
        table = _build_legs_table(matched, show_lots=show_lots)
        console.print(table)

    except click.ClickException:
        raise
    except Exception as exc:
        console.print(f"[red]Error: {exc}[/red]")
        raise click.Abort() from exc
