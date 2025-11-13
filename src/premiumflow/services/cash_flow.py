# file-length-ignore
"""
Account-level cash flow and P&L reporting service.

This module provides functions to aggregate cash flow and profit/loss metrics
across all imports for an account, with time-based grouping (daily, weekly,
monthly, total).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Dict, List, Literal, Optional

from ..core.parser import NormalizedOptionTransaction
from .cash_flow_helpers import (
    ZERO,
    PeriodType,
    _group_date_to_period_key,
    _parse_period_key_to_date,
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


def generate_cash_flow_pnl_report(  # noqa: PLR0913
    repository,
    *,
    account_name: str,
    account_number: Optional[str] = None,
    period_type: PeriodType = "total",
    ticker: Optional[str] = None,
    since: Optional[date] = None,
    until: Optional[date] = None,
    clamp_periods_to_range: bool = True,
    assignment_handling: Literal["include", "exclude"] = "include",
) -> CashFlowPnlReport:
    from .cash_flow_report import generate_cash_flow_pnl_report as _impl

    return _impl(
        repository,
        account_name=account_name,
        account_number=account_number,
        period_type=period_type,
        ticker=ticker,
        since=since,
        until=until,
        clamp_periods_to_range=clamp_periods_to_range,
        assignment_handling=assignment_handling,
    )
