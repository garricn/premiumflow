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
from .leg_matching import (
    MatchedLeg,
    MatchedLegLot,
    _stored_to_normalized,
    group_fills_by_account,
    match_legs_with_errors,
)

CONTRACT_MULTIPLIER = Decimal("100")
ZERO = Decimal("0")
PeriodType = Literal["daily", "weekly", "monthly", "total"]


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
    gross_realized_pnl: Decimal  # Gross realized P&L (before fees) - matches Robinhood
    net_realized_pnl: Decimal  # Realized P&L after fees (actual cash outcome)
    unrealized_exposure: Decimal  # Credit at risk on open positions (premium collected that could be retained if expired worthless)
    gross_pnl: Decimal  # gross_realized_pnl + unrealized_exposure
    net_pnl: Decimal  # net_realized_pnl + unrealized_exposure


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
) -> None:
    """
    Aggregate gross and net realized P&L from closed lots into period_data.

    Realized P&L is attributed to the period when the lot was closed.
    Only includes lots closed within the date range (if filtering).
    """
    for leg in matched_legs:
        for lot in leg.lots:
            if lot.is_closed and lot.realized_pnl is not None:
                if lot.closed_at and _date_in_range(lot.closed_at, since, until):
                    period_key, _ = _group_date_to_period_key(lot.closed_at, period_type)
                    # Aggregate gross realized P&L (before fees)
                    period_data[period_key]["gross_realized_pnl"] += lot.realized_pnl
                    # Aggregate net realized P&L (after fees)
                    if lot.net_pnl is not None:
                        period_data[period_key]["net_realized_pnl"] += lot.net_pnl


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

    Returns
    -------
    Dict[str, Dict[str, Decimal]]
        Dictionary mapping period_key to a dict with 'gross_realized_pnl',
        'net_realized_pnl', and 'unrealized_exposure' keys.
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
        period_data[period_key] = {
            "gross_realized_pnl": ZERO,
            "net_realized_pnl": ZERO,
            "unrealized_exposure": ZERO,
        }

    # Aggregate realized P&L from closed lots
    _aggregate_realized_pnl(matched_legs, period_type, period_data, since=since, until=until)

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
            gross_realized_pnl=ZERO,
            net_realized_pnl=ZERO,
            unrealized_exposure=ZERO,
            gross_pnl=ZERO,
            net_pnl=ZERO,
        ),
    )


def _fetch_and_normalize_transactions(
    repository: SQLiteRepository,
    *,
    account_name: str,
    account_number: Optional[str] = None,
    ticker: Optional[str] = None,
) -> List[NormalizedOptionTransaction]:
    """
    Fetch all transactions and convert them to normalized format.

    Fetches ALL transactions (no date filter) to ensure proper leg matching
    even when opening and closing transactions span date range boundaries.
    """
    all_stored_txns = repository.fetch_transactions(
        account_name=account_name,
        account_number=account_number,
        ticker=ticker,
        since=None,  # No date filter for matching
        until=None,
        status="all",
    )
    return [_stored_to_normalized(stored) for stored in all_stored_txns]


def _match_legs_from_transactions(
    transactions: List[NormalizedOptionTransaction],
) -> List[MatchedLeg]:
    """Match legs from normalized transactions."""
    all_fills = group_fills_by_account(transactions)
    matched_map, _errors = match_legs_with_errors(all_fills)
    return list(matched_map.values())


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
        pnl = pnl_by_period.get(
            period_key,
            {"gross_realized_pnl": ZERO, "net_realized_pnl": ZERO, "unrealized_exposure": ZERO},
        )

        credits = Decimal(cash_flow["credits"])
        debits = Decimal(cash_flow["debits"])
        net_cash_flow = credits - debits
        gross_realized_pnl = Decimal(pnl["gross_realized_pnl"])
        net_realized_pnl = Decimal(pnl["net_realized_pnl"])
        unrealized_exposure = Decimal(pnl["unrealized_exposure"])
        gross_pnl = gross_realized_pnl + unrealized_exposure
        net_pnl = net_realized_pnl + unrealized_exposure

        periods.append(
            PeriodMetrics(
                period_key=period_key,
                period_label=period_label,
                credits=credits,
                debits=debits,
                net_cash_flow=net_cash_flow,
                gross_realized_pnl=gross_realized_pnl,
                net_realized_pnl=net_realized_pnl,
                unrealized_exposure=unrealized_exposure,
                gross_pnl=gross_pnl,
                net_pnl=net_pnl,
            )
        )

    return periods


def _calculate_totals(periods: List[PeriodMetrics]) -> PeriodMetrics:
    """Calculate grand totals across all periods."""
    total_credits = Decimal(sum(p.credits for p in periods))
    total_debits = Decimal(sum(p.debits for p in periods))
    total_gross_realized_pnl = Decimal(sum(p.gross_realized_pnl for p in periods))
    total_net_realized_pnl = Decimal(sum(p.net_realized_pnl for p in periods))
    total_unrealized_exposure = Decimal(sum(p.unrealized_exposure for p in periods))
    total_gross_pnl = total_gross_realized_pnl + total_unrealized_exposure
    total_net_pnl = total_net_realized_pnl + total_unrealized_exposure

    return PeriodMetrics(
        period_key="total",
        period_label="Total",
        credits=total_credits,
        debits=total_debits,
        net_cash_flow=total_credits - total_debits,
        gross_realized_pnl=total_gross_realized_pnl,
        net_realized_pnl=total_net_realized_pnl,
        unrealized_exposure=total_unrealized_exposure,
        gross_pnl=total_gross_pnl,
        net_pnl=total_net_pnl,
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

    Returns
    -------
    CashFlowPnlReport
        Complete report with period-based metrics and totals
    """
    # Fetch and normalize all transactions (no date filter for proper leg matching)
    all_normalized_txns = _fetch_and_normalize_transactions(
        repository,
        account_name=account_name,
        account_number=account_number,
        ticker=ticker,
    )

    if not all_normalized_txns:
        return _create_empty_report(account_name, account_number, period_type)

    # Match legs from all transactions (for proper matching across date boundaries)
    matched_legs = _match_legs_from_transactions(all_normalized_txns)

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
