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
    realized_pnl: Decimal
    unrealized_exposure: Decimal
    total_pnl: Decimal  # realized + unrealized


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
) -> set[str]:
    """
    Collect all period keys that will be needed for P&L aggregation.

    Collects period keys from transactions and matched_legs (from closed lots'
    closed_at dates and open lots' opened_at dates) to ensure all periods are
    initialized upfront.
    """
    all_period_keys: set[str] = set()

    # Collect period keys from transactions
    for txn in transactions:
        period_key, _ = _group_date_to_period_key(txn.activity_date, period_type)
        all_period_keys.add(period_key)

    # Collect period keys from matched_legs
    # - From closed lots: period when they were closed (if within date range)
    # - From open lots: period when they were opened (if overlapping date range)
    for leg in matched_legs:
        for lot in leg.lots:
            if lot.is_closed and lot.realized_pnl is not None:
                if lot.closed_at and _date_in_range(lot.closed_at, since, until):
                    period_key, _ = _group_date_to_period_key(lot.closed_at, period_type)
                    all_period_keys.add(period_key)
            elif lot.is_open:
                if lot.opened_at and _lot_overlaps_date_range(lot.opened_at, until):
                    period_key, _ = _group_date_to_period_key(lot.opened_at, period_type)
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
    Aggregate realized P&L from closed lots into period_data.

    Realized P&L is attributed to the period when the lot was closed.
    Only includes lots closed within the date range (if filtering).
    """
    for leg in matched_legs:
        for lot in leg.lots:
            if lot.is_closed and lot.realized_pnl is not None:
                if lot.closed_at and _date_in_range(lot.closed_at, since, until):
                    period_key, _ = _group_date_to_period_key(lot.closed_at, period_type)
                    period_data[period_key]["realized_pnl"] += lot.realized_pnl


def _aggregate_unrealized_exposure(
    matched_legs: List[MatchedLeg],
    period_type: PeriodType,
    period_data: Dict[str, Dict[str, Decimal]],
    *,
    until: Optional[date] = None,
) -> None:
    """
    Aggregate unrealized exposure from open lots into period_data.

    Unrealized exposure is attributed to the period when each individual lot
    was opened. Includes lots whose lifetime overlaps the date range (opened
    before or during the range).
    """
    for leg in matched_legs:
        for lot in leg.lots:
            if lot.is_open:
                if lot.opened_at and _lot_overlaps_date_range(lot.opened_at, until):
                    period_key, _ = _group_date_to_period_key(lot.opened_at, period_type)
                    period_data[period_key]["unrealized_exposure"] += lot.credit_remaining


def _aggregate_pnl_by_period(
    matched_legs: List[MatchedLeg],
    transactions: List[NormalizedOptionTransaction],
    period_type: PeriodType,
    *,
    since: Optional[date] = None,
    until: Optional[date] = None,
) -> Dict[str, Dict[str, Decimal]]:
    """
    Aggregate realized P&L and unrealized exposure by time period.

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

    Returns
    -------
    Dict[str, Dict[str, Decimal]]
        Dictionary mapping period_key to a dict with 'realized_pnl' and
        'unrealized_exposure' keys.
    """
    # Pre-populate period_data with all relevant period keys from both
    # transactions and matched_legs to ensure all periods are initialized upfront
    all_period_keys = _collect_pnl_period_keys(
        matched_legs, transactions, period_type, since=since, until=until
    )

    # Initialize all periods upfront
    period_data: Dict[str, Dict[str, Decimal]] = {}
    for period_key in all_period_keys:
        period_data[period_key] = {
            "realized_pnl": ZERO,
            "unrealized_exposure": ZERO,
        }

    # Aggregate realized P&L from closed lots
    _aggregate_realized_pnl(matched_legs, period_type, period_data, since=since, until=until)

    # Aggregate unrealized exposure from open lots
    _aggregate_unrealized_exposure(matched_legs, period_type, period_data, until=until)

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
            realized_pnl=ZERO,
            unrealized_exposure=ZERO,
            total_pnl=ZERO,
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

    # Find a transaction in this period to generate the label
    for txn in transactions:
        key, label = _group_date_to_period_key(txn.activity_date, period_type)
        if key == period_key:
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
        pnl = pnl_by_period.get(period_key, {"realized_pnl": ZERO, "unrealized_exposure": ZERO})

        credits = Decimal(cash_flow["credits"])
        debits = Decimal(cash_flow["debits"])
        net_cash_flow = credits - debits
        realized_pnl = Decimal(pnl["realized_pnl"])
        unrealized_exposure = Decimal(pnl["unrealized_exposure"])
        total_pnl = realized_pnl + unrealized_exposure

        periods.append(
            PeriodMetrics(
                period_key=period_key,
                period_label=period_label,
                credits=credits,
                debits=debits,
                net_cash_flow=net_cash_flow,
                realized_pnl=realized_pnl,
                unrealized_exposure=unrealized_exposure,
                total_pnl=total_pnl,
            )
        )

    return periods


def _calculate_totals(periods: List[PeriodMetrics]) -> PeriodMetrics:
    """Calculate grand totals across all periods."""
    total_credits = Decimal(sum(p.credits for p in periods))
    total_debits = Decimal(sum(p.debits for p in periods))
    total_realized_pnl = Decimal(sum(p.realized_pnl for p in periods))
    total_unrealized = Decimal(sum(p.unrealized_exposure for p in periods))

    return PeriodMetrics(
        period_key="total",
        period_label="Total",
        credits=total_credits,
        debits=total_debits,
        net_cash_flow=total_credits - total_debits,
        realized_pnl=total_realized_pnl,
        unrealized_exposure=total_unrealized,
        total_pnl=total_realized_pnl + total_unrealized,
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
        matched_legs, filtered_txns, period_type, since=since, until=until
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
