# file-length-ignore
"""
Account-level cash flow and P&L reporting service.

This module provides functions to aggregate cash flow and profit/loss metrics
across all imports for an account, with time-based grouping (daily, weekly,
monthly, total).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Dict, List, Literal, Optional

from ..core.parser import NormalizedOptionTransaction
from ..persistence import SQLiteRepository
from .cash_flow_helpers import (
    ZERO,
    PeriodType,
    _aggregate_cash_flow_by_period,
    _aggregate_pnl_by_period,
    _build_period_metrics,
    _calculate_totals,
    _date_in_range,
    _group_date_to_period_key,
    _parse_period_key_to_date,
)
from .stock_lots import StockLotSummary, fetch_stock_lot_summaries
from .transaction_loader import (
    fetch_normalized_transactions,
    match_legs_from_transactions,
)

AssignmentHandling = Literal["include", "exclude"]
RealizedView = Literal["options", "stock", "combined"]


@dataclass(frozen=True)
class PeriodMetrics:
    """Cash flow and P&L metrics for a specific time period."""

    period_key: str  # Date string, week identifier, month identifier, or "total"
    period_label: str  # Human-readable label
    credits: Decimal
    debits: Decimal
    net_cash_flow: Decimal
    realized_profits_gross: Decimal  # Sum of positive realized P&L before fees
    realized_losses_gross: Decimal  # Sum of negative realized P&L before fees (absolute value)
    realized_pnl_gross: Decimal  # realized_profits_gross - realized_losses_gross
    realized_profits_net: Decimal  # Sum of positive realized P&L after fees
    realized_losses_net: Decimal  # Sum of negative realized P&L after fees (absolute value)
    realized_pnl_net: Decimal  # realized_profits_net - realized_losses_net
    assignment_realized_gross: Decimal  # Sum of assignment-driven realized P&L before fees
    assignment_realized_net: Decimal  # Assignment-driven realized P&L after fees
    unrealized_exposure: Decimal  # Credit at risk on open positions
    opening_fees: Decimal
    closing_fees: Decimal
    total_fees: Decimal
    realized_breakdowns: Dict[RealizedView, "RealizedViewTotals"]


@dataclass(frozen=True)
class CashFlowPnlReport:
    """Complete account-level cash flow and P&L report."""

    account_name: str
    account_number: Optional[str]
    period_type: PeriodType
    periods: List[PeriodMetrics]
    totals: PeriodMetrics  # Grand totals across all periods


@dataclass(frozen=True)
class RealizedViewTotals:
    """Aggregate realized P&L components for a specific view."""

    profits_gross: Decimal
    losses_gross: Decimal
    net_gross: Decimal
    profits_net: Decimal
    losses_net: Decimal
    net_net: Decimal


def _zero_realized_view_totals() -> RealizedViewTotals:
    """Return a zeroed-out RealizedViewTotals instance."""
    return RealizedViewTotals(
        profits_gross=ZERO,
        losses_gross=ZERO,
        net_gross=ZERO,
        profits_net=ZERO,
        losses_net=ZERO,
        net_net=ZERO,
    )


def _empty_realized_breakdowns() -> Dict[RealizedView, RealizedViewTotals]:
    """Return realized breakdowns with zero values for all views."""
    breakdowns: Dict[RealizedView, RealizedViewTotals] = {
        "options": _zero_realized_view_totals(),
        "stock": _zero_realized_view_totals(),
        "combined": _zero_realized_view_totals(),
    }
    return breakdowns


@dataclass
class _OptionsRealizedTotals:
    """Bundle options realized P&L metrics."""

    profits_gross: Decimal
    losses_gross: Decimal
    pnl_gross: Decimal
    profits_net: Decimal
    losses_net: Decimal
    pnl_net: Decimal


@dataclass
class _StockRealizedTotals:
    """Bundle stock realized P&L metrics."""

    profits: Decimal
    losses: Decimal
    net: Decimal


def _build_realized_breakdowns(
    *,
    options: _OptionsRealizedTotals,
    stock: _StockRealizedTotals,
) -> Dict[RealizedView, RealizedViewTotals]:
    """Construct realized breakdowns for options, stock, and combined views."""
    options_totals = RealizedViewTotals(
        profits_gross=options.profits_gross,
        losses_gross=options.losses_gross,
        net_gross=options.pnl_gross,
        profits_net=options.profits_net,
        losses_net=options.losses_net,
        net_net=options.pnl_net,
    )
    stock_totals = RealizedViewTotals(
        profits_gross=stock.profits,
        losses_gross=stock.losses,
        net_gross=stock.net,
        profits_net=stock.profits,
        losses_net=stock.losses,
        net_net=stock.net,
    )
    combined_totals = RealizedViewTotals(
        profits_gross=options.profits_gross + stock.profits,
        losses_gross=options.losses_gross + stock.losses,
        net_gross=options.pnl_gross + stock.net,
        profits_net=options.profits_net + stock.profits,
        losses_net=options.losses_net + stock.losses,
        net_net=options.pnl_net + stock.net,
    )
    breakdowns: Dict[RealizedView, RealizedViewTotals] = {
        "options": options_totals,
        "stock": stock_totals,
        "combined": combined_totals,
    }
    return breakdowns


def _sum_realized_breakdown(periods: List[PeriodMetrics], view: RealizedView) -> RealizedViewTotals:
    """Sum realized breakdowns for the specified view across all periods."""
    return RealizedViewTotals(
        profits_gross=Decimal(sum(p.realized_breakdowns[view].profits_gross for p in periods)),
        losses_gross=Decimal(sum(p.realized_breakdowns[view].losses_gross for p in periods)),
        net_gross=Decimal(sum(p.realized_breakdowns[view].net_gross for p in periods)),
        profits_net=Decimal(sum(p.realized_breakdowns[view].profits_net for p in periods)),
        losses_net=Decimal(sum(p.realized_breakdowns[view].losses_net for p in periods)),
        net_net=Decimal(sum(p.realized_breakdowns[view].net_net for p in periods)),
    )


def _create_empty_report(
    account_name: str,
    account_number: Optional[str],
    period_type: PeriodType,
) -> CashFlowPnlReport:
    """Create an empty cash flow and P&L report."""
    return CashFlowPnlReport(
        account_name=account_name,
        account_number=account_number,
        period_type=period_type,
        periods=[],
        totals=PeriodMetrics(
            period_key="total",
            period_label="Total",
            credits=ZERO,
            debits=ZERO,
            net_cash_flow=ZERO,
            realized_profits_gross=ZERO,
            realized_losses_gross=ZERO,
            realized_pnl_gross=ZERO,
            realized_profits_net=ZERO,
            realized_losses_net=ZERO,
            realized_pnl_net=ZERO,
            assignment_realized_gross=ZERO,
            assignment_realized_net=ZERO,
            unrealized_exposure=ZERO,
            opening_fees=ZERO,
            closing_fees=ZERO,
            total_fees=ZERO,
            realized_breakdowns=_empty_realized_breakdowns(),
        ),
    )


def _filter_transactions_by_date(
    transactions: List[NormalizedOptionTransaction],
    *,
    since: Optional[date] = None,
    until: Optional[date] = None,
) -> List[NormalizedOptionTransaction]:
    """Filter transactions by date range for cash flow aggregation."""
    if since is None and until is None:
        return transactions

    return [
        txn
        for txn in transactions
        if (since is None or txn.activity_date >= since)
        and (until is None or txn.activity_date <= until)
    ]


def _generate_period_label(
    period_key: str,
    transactions: List[NormalizedOptionTransaction],
    period_type: PeriodType,
) -> str:
    """Generate a human-readable label for a period."""
    if period_key == "total":
        return "Total"

    # Try to find a transaction in this period to generate the label
    for txn in transactions:
        key, label = _group_date_to_period_key(txn.activity_date, period_type)
        if key == period_key:
            return label

    # Fallback: parse period_key to generate label (useful for periods with no transactions)
    period_date = _parse_period_key_to_date(period_key, period_type)
    if period_date:
        _, label = _group_date_to_period_key(period_date, period_type)
        return label

    return period_key


def _parse_stock_lot_closed_date(value: Optional[str]) -> Optional[date]:
    """Parse a persisted stock lot closed_at timestamp into a date."""
    if not value:
        return None
    normalized = value
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            return date.fromisoformat(value.split("T")[0])
        except (ValueError, IndexError):
            return None
    return parsed.date()


def _empty_stock_realized_entry() -> Dict[str, Decimal]:
    """Return a zeroed-out holder for stock realized P&L components."""
    return {"profits": ZERO, "losses": ZERO, "net": ZERO}


def _aggregate_stock_realized_by_period(
    stock_lots: List[StockLotSummary],
    period_type: PeriodType,
    *,
    since: Optional[date] = None,
    until: Optional[date] = None,
) -> Dict[str, Dict[str, Decimal]]:
    """Aggregate realized stock P&L by lot closing period."""
    period_data: Dict[str, Dict[str, Decimal]] = {}

    for lot in stock_lots:
        if lot.status.lower() != "closed":
            continue

        closed_date = _parse_stock_lot_closed_date(lot.closed_at)
        if closed_date is None:
            continue
        if not _date_in_range(closed_date, since, until):
            continue

        period_key, _ = _group_date_to_period_key(closed_date, period_type)
        entry = period_data.setdefault(period_key, _empty_stock_realized_entry())

        realized_total = Decimal(lot.realized_pnl_total)
        if realized_total >= ZERO:
            entry["profits"] += realized_total
        else:
            entry["losses"] += -realized_total
        entry["net"] += realized_total

    return period_data


def generate_cash_flow_pnl_report(  # noqa: PLR0913
    repository: SQLiteRepository,
    *,
    account_name: str,
    account_number: Optional[str] = None,
    period_type: PeriodType = "total",
    ticker: Optional[str] = None,
    since: Optional[date] = None,
    until: Optional[date] = None,
    clamp_periods_to_range: bool = True,
    assignment_handling: AssignmentHandling = "include",
) -> CashFlowPnlReport:
    """
    Generate account-level cash flow and P&L report with time-based grouping.

    Parameters
    ----------
    repository
        SQLite repository for fetching transactions
    account_name
        Account name to filter by
    account_number
        Optional account number for disambiguation
    period_type
        Time period for grouping: "daily", "weekly", "monthly", or "total"
    ticker
        Optional ticker symbol to filter by
    since
        Optional start date for filtering
    until
        Optional end date for filtering
    clamp_periods_to_range
        If True (default), clamp unrealized exposure periods to the first period
        in the range when positions were opened before the range start. This
        ensures that reports only show periods within the requested date range.
        Set to False to show all periods with relevant data, including periods
        before the range start for unrealized exposure.
    assignment_handling
        Whether to include (default) or exclude assignment-derived realized P&L
        from the realized totals. When set to "exclude", assignment premiums are
        tracked separately for reconciliation.

    Returns
    -------
    CashFlowPnlReport
        Complete report with period-based metrics and totals
    """
    # Fetch and normalize all transactions (no date filter for proper leg matching)
    all_normalized_txns = fetch_normalized_transactions(
        repository,
        account_name=account_name,
        account_number=account_number,
        ticker=ticker,
    )

    if not all_normalized_txns:
        return _create_empty_report(account_name, account_number, period_type)

    # Match legs from all transactions (for proper matching across date boundaries)
    matched_legs = match_legs_from_transactions(all_normalized_txns)

    # Filter transactions by date range for cash flow aggregation
    filtered_txns = _filter_transactions_by_date(all_normalized_txns, since=since, until=until)

    # Aggregate realized stock P&L from closed lots
    stock_lot_summaries = fetch_stock_lot_summaries(
        repository,
        account_name=account_name,
        account_number=account_number,
        ticker=ticker,
        status="closed",
    )
    stock_realized_by_period = _aggregate_stock_realized_by_period(
        stock_lot_summaries,
        period_type,
        since=since,
        until=until,
    )

    # Aggregate cash flow and P&L by period
    cash_flow_by_period = _aggregate_cash_flow_by_period(filtered_txns, period_type)
    pnl_by_period = _aggregate_pnl_by_period(
        matched_legs,
        filtered_txns,
        period_type,
        since=since,
        until=until,
        clamp_periods_to_range=clamp_periods_to_range,
        assignment_handling=assignment_handling,
    )

    # Build period metrics and calculate totals
    periods = _build_period_metrics(
        cash_flow_by_period,
        pnl_by_period,
        stock_realized_by_period,
        filtered_txns,
        period_type,
    )
    totals = _calculate_totals(periods)

    return CashFlowPnlReport(
        account_name=account_name,
        account_number=account_number,
        period_type=period_type,
        periods=periods,
        totals=totals,
    )
