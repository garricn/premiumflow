"""
Cash flow and P&L reporting CLI command.

This module provides the `premiumflow cashflow` command for displaying account-level
cash flow and P&L metrics with time-based grouping.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional, cast

import click
from rich.console import Console
from rich.table import Table

from ..persistence import SQLiteRepository
from ..services.cash_flow import (
    AssignmentHandling,
    CashFlowPnlReport,
    RealizedView,
    generate_cash_flow_pnl_report,
)
from ..services.cli_helpers import format_account_label
from ..services.display import format_currency
from ..services.json_serializer import serialize_cash_flow_pnl_report

DateInput = Optional[datetime]
PeriodChoice = click.Choice(["daily", "weekly", "monthly", "total"])
AssignmentHandlingChoice = click.Choice(["include", "exclude"])
RealizedViewChoice = click.Choice(["options", "stock", "combined"])
REALIZED_VIEW_LABELS: dict[RealizedView, str] = {
    "options": "Options",
    "stock": "Stock",
    "combined": "Combined",
}


def _parse_date(value: DateInput) -> Optional[date]:
    """Convert click DateTime to date object."""
    if value is None:
        return None
    return value.date()


def _build_cashflow_table(report: CashFlowPnlReport, realized_view: RealizedView) -> Table:
    """Build a rich.Table for displaying cash flow and P&L metrics."""
    account_label = format_account_label(report.account_name, report.account_number)
    title = f"Cash Flow & P&L Report • {account_label}"
    view_label = REALIZED_VIEW_LABELS.get(realized_view, REALIZED_VIEW_LABELS["options"])

    table = Table(title=title, expand=True)
    table.add_column("Period", style="cyan", no_wrap=True)
    table.add_column("Credits", justify="right")
    table.add_column("Debits", justify="right")
    table.add_column("Net Cash Flow", justify="right")
    table.add_column(f"Profits (Before Fees • {view_label})", justify="right")
    table.add_column(f"Losses (Before Fees • {view_label})", justify="right")
    table.add_column(f"Realized P&L (Before Fees • {view_label})", justify="right")
    table.add_column(f"Profits (After Fees • {view_label})", justify="right")
    table.add_column(f"Losses (After Fees • {view_label})", justify="right")
    table.add_column(f"Realized P&L (After Fees • {view_label})", justify="right")
    table.add_column("Assignment Premium (After Fees)", justify="right")
    table.add_column("Unrealized Exposure", justify="right")
    table.add_column("Opening Fees", justify="right")
    table.add_column("Closing Fees", justify="right")
    table.add_column("Total Fees", justify="right")

    # Add period rows
    for period in report.periods:
        breakdown = (
            period.realized_breakdowns.get(realized_view) or period.realized_breakdowns["options"]
        )
        table.add_row(
            period.period_label,
            format_currency(period.credits),
            format_currency(period.debits),
            format_currency(period.net_cash_flow),
            format_currency(breakdown.profits_gross),
            format_currency(breakdown.losses_gross),
            format_currency(breakdown.net_gross),
            format_currency(breakdown.profits_net),
            format_currency(breakdown.losses_net),
            format_currency(breakdown.net_net),
            format_currency(period.assignment_realized_net),
            format_currency(period.unrealized_exposure),
            format_currency(period.opening_fees),
            format_currency(period.closing_fees),
            format_currency(period.total_fees),
        )

    # Add totals row
    totals_breakdown = (
        report.totals.realized_breakdowns.get(realized_view)
        or report.totals.realized_breakdowns["options"]
    )
    table.add_row(
        report.totals.period_label,
        format_currency(report.totals.credits),
        format_currency(report.totals.debits),
        format_currency(report.totals.net_cash_flow),
        format_currency(totals_breakdown.profits_gross),
        format_currency(totals_breakdown.losses_gross),
        format_currency(totals_breakdown.net_gross),
        format_currency(totals_breakdown.profits_net),
        format_currency(totals_breakdown.losses_net),
        format_currency(totals_breakdown.net_net),
        format_currency(report.totals.assignment_realized_net),
        format_currency(report.totals.unrealized_exposure),
        format_currency(report.totals.opening_fees),
        format_currency(report.totals.closing_fees),
        format_currency(report.totals.total_fees),
        style="bold",
    )

    return table


@click.command()
@click.option(
    "--account-name",
    required=True,
    help="Account name to filter by",
)
@click.option(
    "--account-number",
    required=True,
    help="Account number to filter by",
)
@click.option(
    "--period",
    type=PeriodChoice,
    default="total",
    help="Time period for grouping (default: total)",
)
@click.option(
    "--since",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Start date for filtering (YYYY-MM-DD)",
)
@click.option(
    "--until",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="End date for filtering (YYYY-MM-DD)",
)
@click.option(
    "--ticker",
    help="Filter by ticker symbol",
)
@click.option(
    "--no-clamp-periods",
    "no_clamp_periods",
    is_flag=True,
    default=False,
    help="Don't clamp unrealized exposure periods to date range",
)
@click.option(
    "--json-output",
    "json_output",
    is_flag=True,
    default=False,
    help="Output JSON instead of table",
)
@click.option(
    "--assignment-handling",
    type=AssignmentHandlingChoice,
    default="include",
    show_default=True,
    help=(
        "Include assignment premium in realized totals (include) or track "
        "them separately to match broker UIs (exclude)."
    ),
)
@click.option(
    "--realized-view",
    type=RealizedViewChoice,
    default="options",
    show_default=True,
    help="Select whether realized totals show options, stock, or combined results.",
)
def cashflow(  # noqa: PLR0913
    account_name: str,
    account_number: str,
    period: str,
    since: DateInput,
    until: DateInput,
    ticker: Optional[str],
    no_clamp_periods: bool,
    json_output: bool,
    assignment_handling: AssignmentHandling,
    realized_view: str,
) -> None:
    """Display account-level cash flow and P&L metrics with time-based grouping."""
    console = Console()

    try:
        repo = SQLiteRepository()

        # Parse dates
        since_date = _parse_date(since)
        until_date = _parse_date(until)

        # Generate report
        report = generate_cash_flow_pnl_report(
            repo,
            account_name=account_name,
            account_number=account_number,
            period_type=period,  # type: ignore[arg-type]
            ticker=ticker,
            since=since_date,
            until=until_date,
            clamp_periods_to_range=not no_clamp_periods,
            assignment_handling=assignment_handling,
        )

        # Handle empty state
        if not report.periods:
            if json_output:
                console.print_json(data=serialize_cash_flow_pnl_report(report))
            else:
                console.print(
                    "[yellow]No transactions found matching the specified filters.[/yellow]"
                )
            return

        realized_view_key = cast(RealizedView, realized_view)

        # Output based on format
        if json_output:
            console.print_json(data=serialize_cash_flow_pnl_report(report))
        else:
            table = _build_cashflow_table(report, realized_view_key)
            console.print(table)

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise click.Abort() from e
