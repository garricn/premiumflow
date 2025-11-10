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
    LotFillPortion,
    MatchedLeg,
    _stored_to_normalized,
    group_fills_by_account,
    match_legs_with_errors,
)

DateInput = Optional[datetime]
StatusChoice = click.Choice(["all", "open", "closed"])
FormatChoice = click.Choice(["table", "json"])
LegKey = Tuple[str, Optional[str], str]
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


def _determine_realized_label(realized_pnl: Optional[Decimal]) -> str:
    if realized_pnl is None:
        return "P/L"
    if realized_pnl > 0:
        return "Profit"
    if realized_pnl < 0:
        return "Loss"
    return "P/L"


def _determine_net_label(net_pnl: Decimal) -> str:
    if net_pnl > 0:
        return "Profit"
    if net_pnl < 0:
        return "Loss"
    return "P/L"


def _determine_leg_table_labels(legs: Sequence[MatchedLeg]) -> Tuple[str, str]:
    total_realized = sum((leg.realized_pnl or Decimal("0") for leg in legs), Decimal("0"))
    total_net = sum(
        (leg.realized_pnl or Decimal("0") - leg.total_fees for leg in legs), Decimal("0")
    )

    has_profit = False
    has_loss = False
    for leg in legs:
        if leg.realized_pnl is not None:
            if leg.realized_pnl > 0:
                has_profit = True
            elif leg.realized_pnl < 0:
                has_loss = True

    if has_profit and has_loss:
        return "P/L", "P/L"

    return _determine_realized_label(total_realized), _determine_net_label(total_net)


def _leg_opened_at(leg: MatchedLeg) -> Optional[date]:
    if not leg.lots:
        return None
    return min(lot.opened_at for lot in leg.lots)


def _leg_closed_at(leg: MatchedLeg) -> Optional[date]:
    closed_dates = [lot.closed_at for lot in leg.lots if lot.closed_at is not None]
    if not closed_dates:
        return None
    return max(closed_dates)


def _portion_resolution(portion: LotFillPortion) -> str:
    code = portion.fill.trans_code
    return _CLOSE_LABELS.get(code, code)


def _lot_resolutions(lot) -> List[str]:
    if not lot.close_portions:
        return []
    labels = {_portion_resolution(portion) for portion in lot.close_portions}
    if not labels:
        return []
    if len(labels) == 1:
        return [next(iter(labels))]
    return ["Mixed"]


def _leg_resolution(leg: MatchedLeg) -> str:
    if leg.is_open:
        return "--"

    resolutions: List[str] = []
    for lot in leg.lots:
        if lot.is_closed:
            resolutions.extend(_lot_resolutions(lot))

    if not resolutions:
        return "--"

    unique = set(resolutions)
    if len(unique) == 1:
        return next(iter(unique))
    return "Mixed"


def _build_leg_table(legs: Sequence[MatchedLeg]) -> Table:
    realized_label, net_label = _determine_leg_table_labels(legs)
    table = Table(title="Matched Option Legs", expand=True)
    table.add_column("Account", style="cyan", no_wrap=True)
    table.add_column("Symbol", style="magenta", no_wrap=True)
    table.add_column("Expiration", style="magenta", no_wrap=True)
    table.add_column("Type", style="magenta", no_wrap=True)
    table.add_column("Strike", justify="right")
    table.add_column("Status", style="yellow", no_wrap=True)
    table.add_column("Open Date", style="cyan")
    table.add_column("Open Qty", justify="right")
    table.add_column("Open Credit", justify="right")
    table.add_column("Close Date", style="cyan")
    table.add_column("Close Qty", justify="right")
    table.add_column("Close Cost", justify="right")
    table.add_column(f"Realized {realized_label}", justify="right")
    table.add_column(f"Net {net_label}", justify="right")
    table.add_column("Credit Remaining", justify="right")
    table.add_column("Resolution", style="yellow", no_wrap=True)
    table.add_column("DTE", justify="right")

    totals: Dict[str, Union[int, Decimal]] = {
        "open_qty": 0,
        "close_qty": 0,
        "open_credit": Decimal("0"),
        "close_cost": Decimal("0"),
        "realized": Decimal("0"),
        "net": Decimal("0"),
        "credit_remaining": Decimal("0"),
    }

    for leg in legs:
        account_label = format_account_label(leg.account_name, leg.account_number)
        status = "OPEN" if leg.is_open else "CLOSED"
        opened_at = _format_date(_leg_opened_at(leg))
        closed_at = _format_date(_leg_closed_at(leg)) if not leg.is_open else "--"
        resolution = _leg_resolution(leg)

        total_open_quantity = sum((lot.quantity for lot in leg.lots), 0)
        total_close_quantity = sum((lot.close_quantity for lot in leg.lots), 0)
        total_credit_open = sum((lot.open_credit_gross for lot in leg.lots), Decimal("0"))
        total_close_cost = sum((lot.close_cost for lot in leg.lots), Decimal("0"))
        realized_display = (
            "--" if leg.is_open else format_currency(leg.realized_pnl or Decimal("0"))
        )
        net_value = (leg.realized_pnl or Decimal("0")) - leg.total_fees
        net_display = "--" if leg.is_open else format_currency(net_value)
        credit_remaining = sum((lot.credit_remaining for lot in leg.lots), Decimal("0"))

        table.add_row(
            account_label,
            leg.contract.symbol,
            leg.contract.expiration.isoformat(),
            leg.contract.option_type,
            format_currency(leg.contract.strike),
            status,
            opened_at,
            str(total_open_quantity),
            format_currency(total_credit_open),
            closed_at,
            str(total_close_quantity) if total_close_quantity > 0 else "--",
            format_currency(total_close_cost) if total_close_cost > 0 else "--",
            realized_display,
            net_display,
            format_currency(credit_remaining),
            resolution,
            "N/A" if not leg.is_open else str(leg.days_to_expiration),
        )

        totals["open_qty"] += total_open_quantity  # type: ignore[operator]
        totals["close_qty"] += total_close_quantity  # type: ignore[operator]
        totals["open_credit"] += total_credit_open  # type: ignore[operator]
        totals["close_cost"] += total_close_cost  # type: ignore[operator]
        if not leg.is_open:
            totals["realized"] += leg.realized_pnl or Decimal("0")  # type: ignore[operator]
            totals["net"] += net_value  # type: ignore[operator]
        totals["credit_remaining"] += credit_remaining  # type: ignore[operator]

    table.add_section()
    has_open_legs = any(leg.is_open for leg in legs)
    realized_totals_display = (
        "--" if has_open_legs else format_currency(cast(Decimal, totals["realized"]))
    )
    net_totals_display = "--" if has_open_legs else format_currency(cast(Decimal, totals["net"]))

    table.add_row(
        f"[bold]Totals (Legs: {len(legs)})[/bold]",
        "",
        "",
        "",
        "",
        "",
        "",
        str(totals["open_qty"]),
        format_currency(cast(Decimal, totals["open_credit"])),
        "",
        str(totals["close_qty"]) if totals["close_qty"] > 0 else "--",
        format_currency(cast(Decimal, totals["close_cost"])) if totals["close_cost"] > 0 else "--",
        realized_totals_display,
        net_totals_display,
        format_currency(cast(Decimal, totals["credit_remaining"])),
        "",
        # DTE cannot be meaningfully aggregated; show "N/A" only when all legs are closed
        "N/A" if all(not leg.is_open for leg in legs) else "",
        end_section=True,
    )

    return table


def _describe_portions(portions: Sequence) -> str:
    return (
        ", ".join(
            f"{portion.fill.activity_date.isoformat()} {portion.fill.trans_code} ×{portion.quantity}"
            for portion in portions
        )
        or "--"
    )


def _build_lot_table(leg: MatchedLeg) -> Table:
    title = f"Lots • {leg.contract.display_name} • {format_account_label(leg.account_name, leg.account_number)}"
    table = Table(title=title, expand=True, show_lines=False)
    table.add_column("Status", style="yellow", no_wrap=True)
    table.add_column("Open Date", style="cyan")
    table.add_column("Open Qty", justify="right")
    table.add_column("Open Credit", justify="right")
    table.add_column("Open Fees", justify="right")
    table.add_column("Open Credit Net", justify="right")
    table.add_column("Close Date", style="cyan")
    table.add_column("Close Qty", justify="right")
    table.add_column("Close Cost", justify="right")
    table.add_column("Close Fees", justify="right")
    table.add_column("Close Cost Total", justify="right")
    table.add_column("Realized P/L", justify="right")
    table.add_column("Net P/L", justify="right")
    table.add_column("Credit Remaining", justify="right")
    table.add_column("Qty Remaining", justify="right")
    table.add_column("Total Fees", justify="right")
    table.add_column("Open Portions", overflow="fold")
    table.add_column("Close Portions", overflow="fold")

    totals: Dict[str, Union[int, Decimal]] = {
        "open_quantity": 0,
        "close_quantity": 0,
        "credit_gross": Decimal("0"),
        "open_fees": Decimal("0"),
        "credit_net": Decimal("0"),
        "close_cost": Decimal("0"),
        "close_fees": Decimal("0"),
        "close_cost_total": Decimal("0"),
        "realized": Decimal("0"),
        "net": Decimal("0"),
        "credit_remaining": Decimal("0"),
        "quantity_remaining": 0,
        "total_fees": Decimal("0"),
    }

    for lot in leg.lots:
        realized = lot.realized_pnl
        net_value = lot.net_pnl
        table.add_row(
            lot.status.upper(),
            lot.opened_at.isoformat(),
            str(lot.quantity),
            format_currency(lot.open_credit_gross),
            format_currency(lot.open_fees),
            format_currency(lot.open_credit_net),
            lot.closed_at.isoformat() if lot.closed_at else "--",
            str(lot.close_quantity) if lot.close_quantity else "--",
            format_currency(lot.close_cost) if lot.close_cost > 0 else "--",
            format_currency(lot.close_fees) if lot.close_fees > 0 else "--",
            format_currency(lot.close_cost_total) if lot.close_cost_total > 0 else "--",
            format_currency(realized) if realized is not None else "--",
            format_currency(net_value) if net_value is not None else "--",
            format_currency(lot.credit_remaining),
            str(lot.quantity_remaining),
            format_currency(lot.total_fees),
            _describe_portions(lot.open_portions),
            _describe_portions(lot.close_portions),
        )

        totals["open_quantity"] += lot.quantity  # type: ignore[operator]
        totals["close_quantity"] += lot.close_quantity  # type: ignore[operator]
        totals["credit_gross"] += lot.open_credit_gross  # type: ignore[operator]
        totals["open_fees"] += lot.open_fees  # type: ignore[operator]
        totals["credit_net"] += lot.open_credit_net  # type: ignore[operator]
        totals["close_cost"] += lot.close_cost  # type: ignore[operator]
        totals["close_fees"] += lot.close_fees  # type: ignore[operator]
        totals["close_cost_total"] += lot.close_cost_total  # type: ignore[operator]
        totals["realized"] += lot.realized_pnl or Decimal("0")  # type: ignore[operator]
        totals["net"] += lot.net_pnl or Decimal("0")  # type: ignore[operator]
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
        str(totals["close_quantity"]) if totals["close_quantity"] > 0 else "--",
        format_currency(cast(Decimal, totals["close_cost"])) if totals["close_cost"] > 0 else "--",
        format_currency(cast(Decimal, totals["close_fees"])) if totals["close_fees"] > 0 else "--",
        (
            format_currency(cast(Decimal, totals["close_cost_total"]))
            if totals["close_cost_total"] > 0
            else "--"
        ),
        format_currency(cast(Decimal, totals["realized"])),
        format_currency(cast(Decimal, totals["net"])),
        format_currency(cast(Decimal, totals["credit_remaining"])),
        str(totals["quantity_remaining"]),
        format_currency(cast(Decimal, totals["total_fees"])),
        "",
        "",
        end_section=True,
    )

    return table


@click.command()
@click.option("--account-name", help="Filter by account name")
@click.option("--account-number", help="Filter by account number")
@click.option("--ticker", help="Filter by ticker symbol")
@click.option(
    "--since",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Filter transactions since date (YYYY-MM-DD)",
)
@click.option(
    "--until",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Filter transactions until date (YYYY-MM-DD)",
)
@click.option(
    "--status",
    type=StatusChoice,
    default="all",
    help="Filter by leg status (default: all)",
)
@click.option(
    "--lots",
    "show_lots",
    is_flag=True,
    help="Show detailed lot information",
)
@click.option(
    "--format",
    "output_format",
    type=FormatChoice,
    default="table",
    help="Output format (default: table)",
)
def legs(  # noqa: C901, PLR0913
    account_name: Optional[str],
    account_number: Optional[str],
    ticker: Optional[str],
    since: DateInput,
    until: DateInput,
    status: str,
    show_lots: bool,
    output_format: str,
) -> None:
    """Display matched option legs with FIFO matching."""
    console = Console()

    try:
        repo = SQLiteRepository()
        stored_txns = repo.fetch_transactions(
            account_name=account_name or None,
            account_number=account_number or None,
            ticker=ticker or None,
            since=_parse_date(since),
            until=_parse_date(until),
            status="all",
        )

        if not stored_txns:
            if output_format == "json":
                console.print_json(data={"legs": [], "warnings": []})
            else:
                console.print(
                    "[yellow]No transactions found matching the specified filters.[/yellow]"
                )
            return

        normalized_txns = [_stored_to_normalized(stored) for stored in stored_txns]
        all_fills = group_fills_by_account(normalized_txns)
        matched_map, errors = match_legs_with_errors(all_fills)
        legs_list = _sorted_legs(matched_map.values())

        if status != "all":
            want_open = status == "open"
            legs_list = [leg for leg in legs_list if leg.is_open == want_open]

        warnings: List[str] = []
        for (acct_name, acct_number, leg_id), exc, bucket in errors:
            account_label = format_account_label(acct_name, acct_number)
            # bucket is guaranteed non-empty by match_legs_with_errors structure
            descriptor = bucket[0].transaction.description if bucket else "Unknown"
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

    except click.ClickException:
        raise
    except Exception as exc:  # pragma: no cover - surfaced to user
        console.print(f"[red]Error: {exc}[/red]")
        raise click.Abort() from exc
