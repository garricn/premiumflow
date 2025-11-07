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
from ..persistence import SQLiteRepository
from .leg_matching import MatchedLeg, MatchedLegLot
from .transaction_loader import (
    fetch_normalized_transactions,
    match_legs_from_transactions,
)

CONTRACT_MULTIPLIER = Decimal("100")
ZERO = Decimal("0")
PeriodType = Literal["daily", "weekly", "monthly", "total"]
AssignmentHandling = Literal["include", "exclude"]


def _calculate_cash_value(txn: NormalizedOptionTransaction) -> Decimal:
    """
    Determine the gross cash value of a transaction.

    Prefer broker-supplied ``Amount`` values when available since they already
    include the contract multiplier and reflect the actual cash movement. When
    ``Amount`` is missing (e.g., synthetic fixtures), fall back to
    ``price * quantity * 100`` to approximate the same behaviour.
    """
    if txn.amount is not None:
        return txn.amount

    base_value = txn.price * Decimal(txn.quantity) * CONTRACT_MULTIPLIER
    return base_value if txn.action == "SELL" else -base_value


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


@dataclass(frozen=True)
class CashFlowPnlReport:
    """Complete account-level cash flow and P&L report."""

    account_name: str
    account_number: Optional[str]
    period_type: PeriodType
    periods: List[PeriodMetrics]
    totals: PeriodMetrics  # Grand totals across all periods


def _group_date_to_period_key(
    activity_date: date,
    period_type: PeriodType,
) -> tuple[str, str]:
    """
    Convert an activity date to a period key and label for grouping.

    Returns
    -------
    tuple[str, str]
        (period_key, period_label) where period_key is used for grouping
        and period_label is human-readable.
    """
    if period_type == "daily":
        return (activity_date.isoformat(), activity_date.strftime("%Y-%m-%d"))

    if period_type == "weekly":
        # ISO week format: YYYY-Www
        iso_year, iso_week, _ = activity_date.isocalendar()
        period_key = f"{iso_year}-W{iso_week:02d}"
        period_label = f"Week {iso_week}, {iso_year}"
        return (period_key, period_label)

    if period_type == "monthly":
        period_key = activity_date.strftime("%Y-%m")
        period_label = activity_date.strftime("%B %Y")
        return (period_key, period_label)

    # period_type == "total"
    return ("total", "Total")


def _date_in_range(
    check_date: date, since: Optional[date] = None, until: Optional[date] = None
) -> bool:
    """Check if a date falls within the filter range (if provided)."""
    if since is not None and check_date < since:
        return False
    if until is not None and check_date > until:
        return False
    return True


def _lot_overlaps_date_range(opened_at: date, until: Optional[date] = None) -> bool:
    """
    Check if an open lot's lifetime overlaps the date range.

    An open lot overlaps the range if it was opened before or during the range.
    Since it's open, it's active throughout the range, so we include it if
    opened_at <= until (or until is None).
    """
    if until is not None and opened_at > until:
        return False
    return True


def _lot_was_open_during_period(lot: MatchedLegLot, until: Optional[date] = None) -> bool:
    """
    Check if a lot was open during the requested period.

    A lot was open during the period if:
    - It's currently open (`lot.is_open`), OR
    - It was closed after the period end (`closed_at > until`)

    This ensures historical reports include unrealized exposure for positions
    that were open during the period, even if they were closed afterwards.
    """
    if lot.is_open:
        return True
    if until is None:
        # If no end date, only include currently open lots
        return False
    # Include lots closed after the period end (they were open during the period)
    if lot.closed_at and lot.closed_at > until:
        return True
    return False


def _lot_closed_by_assignment(lot: MatchedLegLot) -> bool:
    """Return True if every closing portion for the lot is an assignment."""
    if not lot.is_closed or not lot.close_portions:
        return False
    return all(portion.fill.is_assignment for portion in lot.close_portions)


def _parse_period_key_to_date(period_key: str, period_type: PeriodType) -> Optional[date]:
    """
    Parse a period_key back to a date for comparison and label generation.

    Parameters
    ----------
    period_key
        The period key to parse (e.g., "2025-10-15", "2025-W42", "2025-10")
    period_type
        The period type (daily, weekly, monthly, total)

    Returns
    -------
    Optional[date]
        The date representing the start of the period, or None if parsing fails
    """
    try:
        if period_type == "daily":
            return date.fromisoformat(period_key)
        elif period_type == "weekly":
            # Parse YYYY-Www format
            year, week = period_key.split("-W")
            return date.fromisocalendar(int(year), int(week), 1)
        elif period_type == "monthly":
            # Parse YYYY-MM format
            year, month = period_key.split("-")
            return date(int(year), int(month), 1)
    except (ValueError, AttributeError):
        return None
    return None


def _clamp_period_to_range(
    period_key: str,
    period_type: PeriodType,
    since: Optional[date],
) -> str:
    """
    Clamp a period_key to the first period in the date range if it's before the range.

    If the period represents a date before `since`, return the period_key for
    the first period in the range. Otherwise, return the original period_key.

    Parameters
    ----------
    period_key
        The period key to potentially clamp
    period_type
        The period type (daily, weekly, monthly, total)
    since
        The start date of the range (if None, no clamping needed)

    Returns
    -------
    str
        The clamped period_key if needed, or the original period_key
    """
    if since is None or period_type == "total":
        return period_key

    # Get the period key for the start date
    first_period_key, _ = _group_date_to_period_key(since, period_type)

    # Convert period keys back to dates for proper comparison
    period_date = _parse_period_key_to_date(period_key, period_type)
    first_date = _parse_period_key_to_date(first_period_key, period_type)

    if period_date is None or first_date is None:
        # Fallback to string comparison if parsing fails
        if period_key < first_period_key:
            return first_period_key
        return period_key

    if period_date < first_date:
        return first_period_key

    return period_key


def _aggregate_cash_flow_by_period(
    transactions: List[NormalizedOptionTransaction],
    period_type: PeriodType,
) -> Dict[str, Dict[str, Decimal]]:
    """
    Aggregate cash flow (credits/debits) by time period.

    Returns
    -------
    Dict[str, Dict[str, Decimal]]
        Dictionary mapping period_key to a dict with 'credits' and 'debits' keys.
    """
    period_data: Dict[str, Dict[str, Decimal]] = {}

    for txn in transactions:
        cash_value = _calculate_cash_value(txn)
        period_key, _ = _group_date_to_period_key(txn.activity_date, period_type)

        if period_key not in period_data:
            period_data[period_key] = {"credits": ZERO, "debits": ZERO}

        if cash_value >= ZERO:
            period_data[period_key]["credits"] += cash_value
        else:
            period_data[period_key]["debits"] += -cash_value

    return period_data


def _collect_pnl_period_keys(
    matched_legs: List[MatchedLeg],
    transactions: List[NormalizedOptionTransaction],
    period_type: PeriodType,
    *,
    since: Optional[date] = None,
    until: Optional[date] = None,
    clamp_periods_to_range: bool = True,
) -> set[str]:
    """
    Collect all period keys that will be needed for P&L aggregation.

    Collects period keys from transactions and matched_legs (from closed lots'
    closed_at dates and open lots' opened_at dates) to ensure all periods are
    initialized upfront.

    Parameters
    ----------
    clamp_periods_to_range
        If True, clamp unrealized exposure periods to the first period in the
        range when positions were opened before the range start.
    """
    all_period_keys: set[str] = set()

    # Collect period keys from transactions
    for txn in transactions:
        period_key, _ = _group_date_to_period_key(txn.activity_date, period_type)
        all_period_keys.add(period_key)

    # Collect period keys from matched_legs
    # - From closed lots: period when they were closed (if within date range)
    # - From lots that were open during the period: period when they were opened (if overlapping date range)
    for leg in matched_legs:
        for lot in leg.lots:
            if lot.is_closed and lot.realized_pnl is not None:
                if lot.closed_at and _date_in_range(lot.closed_at, since, until):
                    period_key, _ = _group_date_to_period_key(lot.closed_at, period_type)
                    all_period_keys.add(period_key)
            # Include lots that were open during the period (currently open or closed after period end)
            if _lot_was_open_during_period(lot, until):
                if lot.opened_at and _lot_overlaps_date_range(lot.opened_at, until):
                    period_key, _ = _group_date_to_period_key(lot.opened_at, period_type)
                    # Clamp period if needed
                    if clamp_periods_to_range:
                        period_key = _clamp_period_to_range(period_key, period_type, since)
                    all_period_keys.add(period_key)

    return all_period_keys


def _aggregate_realized_pnl(
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


def _aggregate_opening_fees(
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


def _aggregate_unrealized_exposure(
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


def _aggregate_pnl_by_period(
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
    filtered_transactions: List[NormalizedOptionTransaction],
    period_type: PeriodType,
) -> List[PeriodMetrics]:
    """Combine cash flow and P&L data into PeriodMetrics objects."""
    all_period_keys = set(cash_flow_by_period.keys()) | set(pnl_by_period.keys())
    periods: List[PeriodMetrics] = []

    for period_key in sorted(all_period_keys):
        period_label = _generate_period_label(period_key, filtered_transactions, period_type)

        cash_flow = cash_flow_by_period.get(period_key, {"credits": ZERO, "debits": ZERO})
        pnl = pnl_by_period.get(period_key, _empty_period_entry())

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
    )


def generate_cash_flow_pnl_report(
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
    periods = _build_period_metrics(cash_flow_by_period, pnl_by_period, filtered_txns, period_type)
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
