"""
Inspect matched option legs from persisted imports.

This module wires the FIFO matching service into a CLI-facing command that renders leg-level
summaries and optional lot details. Data is sourced from the SQLite persistence layer populated via
``premiumflow import``.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Dict, Iterable, List, Optional, Sequence, Tuple, Union, cast

import click
from rich.console import Console
from rich.table import Table

from ..persistence import SQLiteRepository
from ..services.cli_helpers import format_account_label
from ..services.display import format_currency
from ..services.json_serializer import serialize_leg
from ..services.leg_matching import (
    MatchedLeg,
    group_fills_by_account,
    match_legs_with_errors,
)

DateInput = Optional[datetime]
StatusChoice = click.Choice(["all", "open", "closed"])
FormatChoice = click.Choice(["table", "json"])
_CLOSE_LABELS = {
    "BTC": "Buy to close",
    "STC": "Sell to close",
    "OEXP": "Expiration",
    "OASGN": "Assignment",
}


def _parse_date(value: DateInput) -> Optional[date]:
    if value is None:
        return None
    return value.date()


def _format_date(value: Optional[date]) -> str:
    return value.isoformat() if value else "--"


def _sorted_legs(legs: Iterable[MatchedLeg]) -> List[MatchedLeg]:
    return sorted(
        legs,
        key=lambda leg: (
            leg.account_name,
            leg.account_number or "",
            leg.contract.symbol,
            leg.contract.expiration,
            leg.contract.option_type,
            leg.contract.strike,
            leg.contract.leg_id,
        ),
    )


def _determine_realized_label(realized_premium: Optional[Decimal]) -> str:
    """Determine if realized premium represents profit or loss."""
    if realized_premium is None:
        return "P/L"
    if realized_premium > 0:
        return "Profit"
    if realized_premium < 0:
        return "Loss"
    return "P/L"


def _determine_net_label(net_premium: Decimal) -> str:
    """Determine if net premium represents profit or loss."""
    if net_premium > 0:
        return "Profit"
    if net_premium < 0:
        return "Loss"
    return "P/L"


def _determine_leg_table_labels(legs: Sequence[MatchedLeg]) -> Tuple[str, str]:
    """Determine profit/loss labels for leg table based on totals."""
    total_realized = sum((leg.realized_premium or Decimal("0") for leg in legs), Decimal("0"))
    total_net = sum((leg.net_premium for leg in legs), Decimal("0"))

    # Check if we have mixed profit/loss legs
    has_profit = False
    has_loss = False
    for leg in legs:
        if leg.realized_premium is not None:
            if leg.realized_premium > 0:
                has_profit = True
            elif leg.realized_premium < 0:
                has_loss = True

    # If we have both profit and loss legs, use "P/L"
    if has_profit and has_loss:
        realized_label = "P/L"
        net_label = "P/L"
    else:
        realized_label = _determine_realized_label(total_realized)
        net_label = _determine_net_label(total_net)

    return realized_label, net_label


def _build_leg_table(legs: Sequence[MatchedLeg]) -> Table:
    # Determine labels based on overall profit/loss
    realized_label, net_label = _determine_leg_table_labels(legs)
    table = Table(title="Matched Option Legs", expand=True)
    table.add_column("Account", style="cyan", no_wrap=True)
    table.add_column("Symbol", style="magenta", no_wrap=True)
    table.add_column("Expiration", style="magenta", no_wrap=True)
    table.add_column("Type", style="magenta", no_wrap=True)
    table.add_column("Strike", justify="right")
    table.add_column("Status", style="yellow", no_wrap=True)
    table.add_column("Open Date", style="cyan")
    table.add_column("Open Quantity", justify="right")
    table.add_column("Open Credit Gross", justify="right")
    table.add_column("Close Date", style="cyan")
    table.add_column("Close Quantity", justify="right")
    table.add_column("Close Cost", justify="right")
    table.add_column(f"Realized {realized_label}", justify="right")
    table.add_column("Credit Remaining", justify="right")
    table.add_column("Resolution", style="yellow", no_wrap=True)
    table.add_column("DTE", justify="right")

    totals: Dict[str, Union[int, Decimal]] = {
        "open_qty": 0,
        "close_qty": 0,
        "credit_open": Decimal("0"),
        "cost_close": Decimal("0"),
        "realized": Decimal("0"),
        "credit_remaining": Decimal("0"),
    }

    for leg in legs:
        account_label = format_account_label(leg.account_name, leg.account_number)
        status = "OPEN" if leg.is_open else "CLOSED"
        opened_at = _format_date(leg.opened_at)
        closed_at = _format_date(leg.closed_at) if not leg.is_open else "--"
        resolution = leg.resolution()
        # Use domain model properties (single source of truth)
        credit_remaining = leg.open_premium

        # Show realized P/L if there are any closed lots (fixes Comment 1)
        has_closed_lots = any(lot.is_closed for lot in leg.lots)
        realized_display = format_currency(leg.realized_premium) if has_closed_lots else "--"

        table.add_row(
            account_label,
            leg.contract.symbol,
            leg.contract.expiration.isoformat(),
            leg.contract.option_type,
            format_currency(leg.contract.strike),
            status,
            opened_at,
            str(leg.opened_quantity),
            format_currency(leg.open_credit_gross),
            closed_at,
            str(leg.closed_quantity) if leg.closed_quantity > 0 else "--",
            format_currency(leg.close_cost) if leg.close_cost > 0 else "--",
            realized_display,
            format_currency(credit_remaining),
            resolution,
            "N/A" if not leg.is_open else str(leg.days_to_expiration),
        )

        totals["open_qty"] += leg.opened_quantity  # type: ignore[operator]
        totals["close_qty"] += leg.closed_quantity  # type: ignore[operator]
        totals["credit_open"] += leg.open_credit_gross  # type: ignore[operator]
        totals["cost_close"] += leg.close_cost  # type: ignore[operator]
        # Include realized_premium from partially closed legs (fixes Comment 1)
        if has_closed_lots:
            totals["realized"] += leg.realized_premium or Decimal("0")  # type: ignore[operator]
        totals["credit_remaining"] += credit_remaining  # type: ignore[operator]

    table.add_section()
    # Determine totals labels - show realized if any legs have closed lots
    has_any_closed_lots = any(any(lot.is_closed for lot in leg.lots) for leg in legs)
    realized_totals_display = (
        format_currency(cast(Decimal, totals["realized"])) if has_any_closed_lots else "--"
    )

    table.add_row(
        f"[bold]Totals (Legs: {len(legs)})[/bold]",
        "",
        "",
        "",
        "",
        "",
        "",
        str(totals["open_qty"]),
        format_currency(cast(Decimal, totals["credit_open"])),
        "",
        str(totals["close_qty"]) if totals["close_qty"] > 0 else "--",
        format_currency(cast(Decimal, totals["cost_close"])) if totals["cost_close"] > 0 else "--",
        realized_totals_display,
        format_currency(cast(Decimal, totals["credit_remaining"])),
        "",
        "N/A" if all(not leg.is_open for leg in legs) else "",
        end_section=True,
    )

    return table


def _describe_portions(portions: Sequence) -> str:
    """Summarise the fills that compose a lot portion."""
    return (
        ", ".join(
            f"{portion.fill.activity_date.isoformat()} {portion.fill.trans_code} ×{portion.quantity}"
            for portion in portions
        )
        or "--"
    )


def _build_lot_table(leg: MatchedLeg) -> Table:
    table = Table(
        title=f"Lots • {leg.contract.display_name} • {format_account_label(leg.account_name, leg.account_number)}",
        expand=True,
        show_lines=False,
    )
    table.add_column("Status", style="yellow", no_wrap=True)
    table.add_column("Open Date", style="cyan")
    table.add_column("Open Quantity", justify="right")
    table.add_column("Open Credit Gross", justify="right")
    table.add_column("Open Fees", justify="right")
    table.add_column("Open Credit Net", justify="right")
    table.add_column("Close Date", style="cyan")
    table.add_column("Close Quantity", justify="right")
    table.add_column("Close Cost", justify="right")
    table.add_column("Close Fees", justify="right")
    table.add_column("Close Cost Total", justify="right")
    table.add_column("Realized P/L", justify="right")
    table.add_column("Net P/L", justify="right")
    table.add_column("Credit Remaining", justify="right")
    table.add_column("Quantity Remaining", justify="right")
    table.add_column("Total Fees", justify="right")
    table.add_column("DTE", justify="right")

    totals: Dict[str, Union[int, Decimal]] = {
        "open_quantity": 0,
        "close_quantity": 0,
        "credit_gross": Decimal("0"),
        "open_fees": Decimal("0"),
        "credit_net": Decimal("0"),
        "cost": Decimal("0"),
        "close_fees": Decimal("0"),
        "cost_total": Decimal("0"),
        "realized": Decimal("0"),
        "net": Decimal("0"),
        "credit_remaining": Decimal("0"),
        "quantity_remaining": 0,
        "total_fees": Decimal("0"),
    }

    for lot in leg.lots:
        opened_at = lot.opened_at.isoformat()
        closed_at = lot.closed_at.isoformat() if lot.closed_at else "--"
        dte = str(lot.contract.days_to_expiration()) if lot.quantity_remaining > 0 else "N/A"

        table.add_row(
            lot.status.upper(),
            opened_at,
            str(lot.quantity),
            format_currency(lot.open_credit_gross),
            format_currency(lot.open_fees),
            format_currency(lot.open_credit_net),
            closed_at,
            str(lot.close_quantity),
            format_currency(lot.close_cost) if lot.close_cost > 0 else "--",
            format_currency(lot.close_fees) if lot.close_fees > 0 else "--",
            format_currency(lot.close_cost_total) if lot.close_cost_total > 0 else "--",
            format_currency(lot.realized_premium) if lot.realized_premium is not None else "--",
            format_currency(lot.net_premium) if lot.net_premium is not None else "--",
            format_currency(lot.credit_remaining),
            str(lot.quantity_remaining),
            format_currency(lot.total_fees),
            dte,
        )

        totals["open_quantity"] += lot.quantity  # type: ignore[operator]
        totals["close_quantity"] += lot.close_quantity  # type: ignore[operator]
        totals["credit_gross"] += lot.open_credit_gross  # type: ignore[operator]
        totals["open_fees"] += lot.open_fees  # type: ignore[operator]
        totals["credit_net"] += lot.open_credit_net  # type: ignore[operator]
        totals["cost"] += lot.close_cost  # type: ignore[operator]
        totals["close_fees"] += lot.close_fees  # type: ignore[operator]
        totals["cost_total"] += lot.close_cost_total  # type: ignore[operator]
        totals["realized"] += lot.realized_premium or Decimal("0")  # type: ignore[operator]
        totals["net"] += lot.net_premium or Decimal("0")  # type: ignore[operator]
        totals["credit_remaining"] += lot.credit_remaining  # type: ignore[operator]
        totals["quantity_remaining"] += lot.quantity_remaining  # type: ignore[operator]
        totals["total_fees"] += lot.total_fees  # type: ignore[operator]

    table.add_section()
    table.add_row(
        f"[bold]Totals (Lots: {len(leg.lots)})[/bold]",
        "",
        str(totals["open_quantity"]),
        format_currency(cast(Decimal, totals["credit_gross"])),
        format_currency(cast(Decimal, totals["open_fees"])),
        format_currency(cast(Decimal, totals["credit_net"])),
        "",
        str(totals["close_quantity"]),
        format_currency(cast(Decimal, totals["cost"])),
        format_currency(cast(Decimal, totals["close_fees"])),
        format_currency(cast(Decimal, totals["cost_total"])),
        format_currency(cast(Decimal, totals["realized"])),
        format_currency(cast(Decimal, totals["net"])),
        format_currency(cast(Decimal, totals["credit_remaining"])),
        str(totals["quantity_remaining"]),
        format_currency(cast(Decimal, totals["total_fees"])),
        "N/A" if totals["quantity_remaining"] == 0 else str(leg.contract.days_to_expiration()),
        end_section=True,
    )

    return table


@click.command()
@click.option("--account-name", help="Filter by account name recorded during import.")
@click.option("--account-number", help="Filter by account number recorded during import.")
@click.option("--ticker", help="Filter by option underlying symbol (case-insensitive).")
@click.option(
    "--since",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Only include transactions on or after this date (YYYY-MM-DD).",
)
@click.option(
    "--until",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Only include transactions on or before this date (YYYY-MM-DD).",
)
@click.option(
    "--status",
    type=StatusChoice,
    default="all",
    show_default=True,
    help="Filter legs by open/closed status.",
)
@click.option(
    "--format",
    "output_format",
    type=FormatChoice,
    default="table",
    show_default=True,
    help="Output format.",
)
@click.option(
    "--lots",
    "show_lots",
    is_flag=True,
    help="Render per-lot detail beneath each leg in table output.",
)
def legs(
    account_name: Optional[str],
    account_number: Optional[str],
    ticker: Optional[str],
    since: DateInput,
    until: DateInput,
    status: str,
    output_format: str,
    show_lots: bool,
) -> None:
    """Inspect matched legs built from persisted imports."""
    console = Console()
    repository = SQLiteRepository()

    transactions = repository.fetch_transactions(
        account_name=account_name or None,
        account_number=account_number or None,
        ticker=ticker or None,
        since=_parse_date(since),
        until=_parse_date(until),
    )

    if not transactions:
        console.print("[yellow]No transactions found for the requested filters.[/yellow]")
        return

    fills = group_fills_by_account(transactions)
    matched_map, errors = match_legs_with_errors(fills)
    legs_list = _sorted_legs(matched_map.values())

    if status != "all":
        want_open = status == "open"
        legs_list = [leg for leg in legs_list if leg.is_open == want_open]

    warnings: List[str] = []
    for (acct_name, acct_number, leg_id), exc, bucket in errors:
        account_label = format_account_label(acct_name, acct_number)
        descriptor = bucket[0].transaction.description
        warnings.append(f"{account_label} • {leg_id} • {descriptor}: {exc}")

    if output_format == "json":
        payload = {
            "legs": [serialize_leg(leg) for leg in legs_list],
            "warnings": warnings,
        }
        console.print_json(data=payload)
        return

    if legs_list:
        table = _build_leg_table(legs_list)
        console.print(table)

        if show_lots:
            for leg in legs_list:
                lot_table = _build_lot_table(leg)
                console.print(lot_table)
    else:
        console.print("[yellow]No matched legs match the requested filters.[/yellow]")

    if warnings:
        console.print("\n[red]Warnings:[/red]")
        for message in warnings:
            console.print(f"- {message}")


__all__ = ["legs"]
