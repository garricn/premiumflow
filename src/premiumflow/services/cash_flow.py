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
from .cash_flow.helpers import (
    ZERO,
    PeriodType,
    _aggregate_cash_flow_by_period,
    _clamp_period_to_range,
    _collect_pnl_period_keys,
    _date_in_range,
    _group_date_to_period_key,
    _lot_closed_by_assignment,
    _lot_overlaps_date_range,
    _lot_was_open_during_period,
    _parse_period_key_to_date,
)
from .leg_matching import MatchedLeg
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


def _build_realized_breakdowns(  # noqa: PLR0913
    *,
    options_profits_gross: Decimal,
    options_losses_gross: Decimal,
    options_pnl_gross: Decimal,
    options_profits_net: Decimal,
    options_losses_net: Decimal,
    options_pnl_net: Decimal,
    stock_profits: Decimal,
    stock_losses: Decimal,
    stock_net: Decimal,
) -> Dict[RealizedView, RealizedViewTotals]:
    """Construct realized breakdowns for options, stock, and combined views."""
    options_totals = RealizedViewTotals(
        profits_gross=options_profits_gross,
        losses_gross=options_losses_gross,
        net_gross=options_pnl_gross,
        profits_net=options_profits_net,
        losses_net=options_losses_net,
        net_net=options_pnl_net,
    )
    stock_totals = RealizedViewTotals(
        profits_gross=stock_profits,
        losses_gross=stock_losses,
        net_gross=stock_net,
        profits_net=stock_profits,
        losses_net=stock_losses,
        net_net=stock_net,
    )
    combined_totals = RealizedViewTotals(
        profits_gross=options_profits_gross + stock_profits,
        losses_gross=options_losses_gross + stock_losses,
        net_gross=options_pnl_gross + stock_net,
        profits_net=options_profits_net + stock_profits,
        losses_net=options_losses_net + stock_losses,
        net_net=options_pnl_net + stock_net,
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


def _aggregate_realized_pnl(  # noqa: C901, PLR0913
    matched_legs: List[MatchedLeg],
    period_type: PeriodType,
    period_data: Dict[str, Dict[str, Decimal]],
    *,
    since: Optional[date] = None,
    until: Optional[date] = None,
    assignment_handling: AssignmentHandling = "include",
) -> None:
    """
    Aggregate realized P&L (gross and net) from closed lots into period_data.

    Realized P&L is attributed to the period when the lot was closed.
    Only includes lots closed within the date range (if filtering).
    """
    for leg in matched_legs:
        for lot in leg.lots:
            if lot.is_closed and lot.realized_pnl is not None and lot.closed_at:
                if not _date_in_range(lot.closed_at, since, until):
                    continue

                period_key, _ = _group_date_to_period_key(lot.closed_at, period_type)

                is_assignment_close = _lot_closed_by_assignment(lot)
                include_assignment = not (is_assignment_close and assignment_handling == "exclude")

                gross_realized = Decimal(lot.realized_pnl)
                if is_assignment_close:
                    period_data[period_key]["assignment_realized_gross"] += gross_realized
                if include_assignment:
                    if gross_realized >= ZERO:
                        period_data[period_key]["realized_profits_gross"] += gross_realized
                    else:
                        period_data[period_key]["realized_losses_gross"] += -gross_realized
                    period_data[period_key]["realized_pnl_gross"] += gross_realized

                if lot.net_pnl is None:
                    continue

                net_realized = Decimal(lot.net_pnl)
                if is_assignment_close:
                    period_data[period_key]["assignment_realized_net"] += net_realized
                if include_assignment:
                    if net_realized >= ZERO:
                        period_data[period_key]["realized_profits_net"] += net_realized
                    else:
                        period_data[period_key]["realized_losses_net"] += -net_realized
                    period_data[period_key]["realized_pnl_net"] += net_realized


def _aggregate_opening_fees(  # noqa: C901, PLR0913
    matched_legs: List[MatchedLeg],
    period_type: PeriodType,
    period_data: Dict[str, Dict[str, Decimal]],
    *,
    since: Optional[date] = None,
    until: Optional[date] = None,
    clamp_periods_to_range: bool = True,
) -> None:
    """Aggregate opening fees by the period in which each lot was opened."""
    for leg in matched_legs:
        for lot in leg.lots:
            if lot.opened_at is None:
                continue
            if since is not None and lot.closed_at and lot.closed_at < since:
                # Lot lifetime ends before the requested window—skip entirely
                continue

            period_key, _ = _group_date_to_period_key(lot.opened_at, period_type)
            if since is not None and lot.opened_at < since:
                if not clamp_periods_to_range:
                    continue
                period_key = _clamp_period_to_range(period_key, period_type, since)
            elif not _date_in_range(lot.opened_at, since, until):
                continue

            open_fees = Decimal(lot.open_fees)
            if open_fees == ZERO:
                continue
            if period_key not in period_data:
                if not clamp_periods_to_range:
                    continue
                period_data[period_key] = _empty_period_entry()
            period_data[period_key]["opening_fees"] += open_fees
            period_data[period_key]["total_fees"] += open_fees


def _aggregate_closing_fees(
    matched_legs: List[MatchedLeg],
    period_type: PeriodType,
    period_data: Dict[str, Dict[str, Decimal]],
    *,
    since: Optional[date] = None,
    until: Optional[date] = None,
) -> None:
    """Aggregate closing fees by the period in which each lot was closed."""
    for leg in matched_legs:
        for lot in leg.lots:
            if not lot.is_closed or lot.closed_at is None:
                continue
            if not _date_in_range(lot.closed_at, since, until):
                continue

            period_key, _ = _group_date_to_period_key(lot.closed_at, period_type)
            close_fees = Decimal(lot.close_fees)
            period_data[period_key]["closing_fees"] += close_fees
            period_data[period_key]["total_fees"] += close_fees


def _aggregate_unrealized_exposure(  # noqa: PLR0913
    matched_legs: List[MatchedLeg],
    period_type: PeriodType,
    period_data: Dict[str, Dict[str, Decimal]],
    *,
    since: Optional[date] = None,
    until: Optional[date] = None,
    clamp_periods_to_range: bool = True,
) -> None:
    """
    Aggregate unrealized exposure from lots that were open during the period.

    Unrealized exposure (credit at risk) is attributed to the period when each
    individual lot was opened. Includes lots whose lifetime overlaps the date
    range (opened before or during the range). Also includes lots that were
    open during the period but closed afterwards (for historical reports).

    Note: This represents the premium collected that could be retained if the
    position expires worthless, not the mark-to-market unrealized P&L.

    Parameters
    ----------
    clamp_periods_to_range
        If True, clamp unrealized exposure periods to the first period in the
        range when positions were opened before the range start.
    """
    for leg in matched_legs:
        for lot in leg.lots:
            # Include lots that were open during the period (currently open or closed after period end)
            if _lot_was_open_during_period(lot, until):
                if lot.opened_at and _lot_overlaps_date_range(lot.opened_at, until):
                    period_key, _ = _group_date_to_period_key(lot.opened_at, period_type)
                    # Clamp period if needed
                    if clamp_periods_to_range:
                        period_key = _clamp_period_to_range(period_key, period_type, since)
                    # Use open_premium for unrealized exposure (credit_remaining is 0 for closed lots)
                    # For open lots, credit_remaining equals open_premium, so this works for both
                    exposure = lot.credit_remaining if lot.is_open else lot.open_premium
                    period_data[period_key]["unrealized_exposure"] += exposure


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


def _aggregate_pnl_by_period(  # noqa: PLR0913
    matched_legs: List[MatchedLeg],
    transactions: List[NormalizedOptionTransaction],
    period_type: PeriodType,
    *,
    since: Optional[date] = None,
    until: Optional[date] = None,
    clamp_periods_to_range: bool = True,
    assignment_handling: AssignmentHandling = "include",
) -> Dict[str, Dict[str, Decimal]]:
    """
    Aggregate gross/net realized P&L and unrealized exposure by time period.

    Parameters
    ----------
    matched_legs
        All matched legs (from full account history for proper matching)
    transactions
        Filtered transactions (for period initialization)
    period_type
        Time period type for grouping
    since
        Optional start date filter - only include P&L within this range
    until
        Optional end date filter - only include P&L within this range
    clamp_periods_to_range
        If True, clamp unrealized exposure periods to the first period in the
        range when positions were opened before the range start.
    assignment_handling
        Whether assignment-driven realized P&L should be included in the main
        realized totals ("include") or tracked separately ("exclude").

    Returns
    -------
    Dict[str, Dict[str, Decimal]]
        Dictionary mapping period_key to realized profit/loss, exposure, and fee data.
    """
    # Pre-populate period_data with all relevant period keys from both
    # transactions and matched_legs to ensure all periods are initialized upfront
    all_period_keys = _collect_pnl_period_keys(
        matched_legs,
        transactions,
        period_type,
        since=since,
        until=until,
        clamp_periods_to_range=clamp_periods_to_range,
    )

    # Initialize all periods upfront
    period_data: Dict[str, Dict[str, Decimal]] = {}
    for period_key in all_period_keys:
        period_data[period_key] = _empty_period_entry()

    # Aggregate realized P&L from closed lots
    _aggregate_realized_pnl(
        matched_legs,
        period_type,
        period_data,
        since=since,
        until=until,
        assignment_handling=assignment_handling,
    )

    # Aggregate fees
    _aggregate_opening_fees(
        matched_legs,
        period_type,
        period_data,
        since=since,
        until=until,
        clamp_periods_to_range=clamp_periods_to_range,
    )
    _aggregate_closing_fees(matched_legs, period_type, period_data, since=since, until=until)

    # Aggregate unrealized exposure from lots that were open during the period
    _aggregate_unrealized_exposure(
        matched_legs,
        period_type,
        period_data,
        since=since,
        until=until,
        clamp_periods_to_range=clamp_periods_to_range,
    )

    return period_data


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


def _build_period_metrics(
    cash_flow_by_period: Dict[str, Dict[str, Decimal]],
    pnl_by_period: Dict[str, Dict[str, Decimal]],
    stock_realized_by_period: Dict[str, Dict[str, Decimal]],
    filtered_transactions: List[NormalizedOptionTransaction],
    period_type: PeriodType,
) -> List[PeriodMetrics]:
    """Combine cash flow and P&L data into PeriodMetrics objects."""
    all_period_keys = (
        set(cash_flow_by_period.keys())
        | set(pnl_by_period.keys())
        | set(stock_realized_by_period.keys())
    )
    periods: List[PeriodMetrics] = []

    for period_key in sorted(all_period_keys):
        period_label = _generate_period_label(period_key, filtered_transactions, period_type)

        cash_flow = cash_flow_by_period.get(period_key, {"credits": ZERO, "debits": ZERO})
        pnl = pnl_by_period.get(period_key, _empty_period_entry())
        stock_realized = stock_realized_by_period.get(period_key, _empty_stock_realized_entry())

        credits = Decimal(cash_flow["credits"])
        debits = Decimal(cash_flow["debits"])
        net_cash_flow = credits - debits
        realized_profits_gross = Decimal(pnl["realized_profits_gross"])
        realized_losses_gross = Decimal(pnl["realized_losses_gross"])
        realized_pnl_gross = Decimal(pnl["realized_pnl_gross"])
        realized_profits_net = Decimal(pnl["realized_profits_net"])
        realized_losses_net = Decimal(pnl["realized_losses_net"])
        realized_pnl_net = Decimal(pnl["realized_pnl_net"])
        assignment_realized_gross = Decimal(pnl["assignment_realized_gross"])
        assignment_realized_net = Decimal(pnl["assignment_realized_net"])
        unrealized_exposure = Decimal(pnl["unrealized_exposure"])
        opening_fees = Decimal(pnl["opening_fees"])
        closing_fees = Decimal(pnl["closing_fees"])
        total_fees = Decimal(pnl["total_fees"])
        stock_profits = Decimal(stock_realized["profits"])
        stock_losses = Decimal(stock_realized["losses"])
        stock_net = Decimal(stock_realized["net"])

        realized_breakdowns = _build_realized_breakdowns(
            options_profits_gross=realized_profits_gross,
            options_losses_gross=realized_losses_gross,
            options_pnl_gross=realized_pnl_gross,
            options_profits_net=realized_profits_net,
            options_losses_net=realized_losses_net,
            options_pnl_net=realized_pnl_net,
            stock_profits=stock_profits,
            stock_losses=stock_losses,
            stock_net=stock_net,
        )

        periods.append(
            PeriodMetrics(
                period_key=period_key,
                period_label=period_label,
                credits=credits,
                debits=debits,
                net_cash_flow=net_cash_flow,
                realized_profits_gross=realized_profits_gross,
                realized_losses_gross=realized_losses_gross,
                realized_pnl_gross=realized_pnl_gross,
                realized_profits_net=realized_profits_net,
                realized_losses_net=realized_losses_net,
                realized_pnl_net=realized_pnl_net,
                assignment_realized_gross=assignment_realized_gross,
                assignment_realized_net=assignment_realized_net,
                unrealized_exposure=unrealized_exposure,
                opening_fees=opening_fees,
                closing_fees=closing_fees,
                total_fees=total_fees,
                realized_breakdowns=realized_breakdowns,
            )
        )

    return periods


def _calculate_totals(periods: List[PeriodMetrics]) -> PeriodMetrics:
    """Calculate grand totals across all periods."""
    total_credits = Decimal(sum(p.credits for p in periods))
    total_debits = Decimal(sum(p.debits for p in periods))
    total_realized_profits_gross = Decimal(sum(p.realized_profits_gross for p in periods))
    total_realized_losses_gross = Decimal(sum(p.realized_losses_gross for p in periods))
    total_realized_pnl_gross = Decimal(sum(p.realized_pnl_gross for p in periods))
    total_realized_profits_net = Decimal(sum(p.realized_profits_net for p in periods))
    total_realized_losses_net = Decimal(sum(p.realized_losses_net for p in periods))
    total_realized_pnl_net = Decimal(sum(p.realized_pnl_net for p in periods))
    total_assignment_realized_gross = Decimal(sum(p.assignment_realized_gross for p in periods))
    total_assignment_realized_net = Decimal(sum(p.assignment_realized_net for p in periods))
    total_unrealized_exposure = Decimal(sum(p.unrealized_exposure for p in periods))
    total_opening_fees = Decimal(sum(p.opening_fees for p in periods))
    total_closing_fees = Decimal(sum(p.closing_fees for p in periods))
    total_fees = total_opening_fees + total_closing_fees
    stock_totals = _sum_realized_breakdown(periods, "stock")
    combined_totals = _sum_realized_breakdown(periods, "combined")
    options_totals = RealizedViewTotals(
        profits_gross=total_realized_profits_gross,
        losses_gross=total_realized_losses_gross,
        net_gross=total_realized_pnl_gross,
        profits_net=total_realized_profits_net,
        losses_net=total_realized_losses_net,
        net_net=total_realized_pnl_net,
    )
    realized_breakdowns: Dict[RealizedView, RealizedViewTotals] = {
        "options": options_totals,
        "stock": stock_totals,
        "combined": combined_totals,
    }

    return PeriodMetrics(
        period_key="total",
        period_label="Total",
        credits=total_credits,
        debits=total_debits,
        net_cash_flow=total_credits - total_debits,
        realized_profits_gross=total_realized_profits_gross,
        realized_losses_gross=total_realized_losses_gross,
        realized_pnl_gross=total_realized_pnl_gross,
        realized_profits_net=total_realized_profits_net,
        realized_losses_net=total_realized_losses_net,
        realized_pnl_net=total_realized_pnl_net,
        assignment_realized_gross=total_assignment_realized_gross,
        assignment_realized_net=total_assignment_realized_net,
        unrealized_exposure=total_unrealized_exposure,
        opening_fees=total_opening_fees,
        closing_fees=total_closing_fees,
        total_fees=total_fees,
        realized_breakdowns=realized_breakdowns,
    )


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


def _empty_period_entry() -> Dict[str, Decimal]:
    """Return a zeroed-out holder for per-period P&L components."""
    return {
        "realized_profits_gross": ZERO,
        "realized_losses_gross": ZERO,
        "realized_pnl_gross": ZERO,
        "realized_profits_net": ZERO,
        "realized_losses_net": ZERO,
        "realized_pnl_net": ZERO,
        "assignment_realized_gross": ZERO,
        "assignment_realized_net": ZERO,
        "unrealized_exposure": ZERO,
        "opening_fees": ZERO,
        "closing_fees": ZERO,
        "total_fees": ZERO,
    }
