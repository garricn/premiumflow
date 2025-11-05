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


def _aggregate_pnl_by_period(
    matched_legs: List[MatchedLeg],
    transactions: List[NormalizedOptionTransaction],
    period_type: PeriodType,
) -> Dict[str, Dict[str, Decimal]]:
    """
    Aggregate realized P&L and unrealized exposure by time period.

    Returns
    -------
    Dict[str, Dict[str, Decimal]]
        Dictionary mapping period_key to a dict with 'realized_pnl' and
        'unrealized_exposure' keys.
    """
    period_data: Dict[str, Dict[str, Decimal]] = {}

    # Create a lookup map: transaction date -> period key
    # We'll use this to determine which period a lot's P&L belongs to
    txn_date_to_period: Dict[date, str] = {}
    for txn in transactions:
        period_key, _ = _group_date_to_period_key(txn.activity_date, period_type)
        txn_date_to_period[txn.activity_date] = period_key

    # Initialize all periods we've seen
    for period_key in set(txn_date_to_period.values()):
        period_data[period_key] = {
            "realized_pnl": ZERO,
            "unrealized_exposure": ZERO,
        }

    # Aggregate realized P&L from closed lots
    # Realized P&L should be attributed to the period when the lot was closed
    for leg in matched_legs:
        for lot in leg.lots:
            if lot.is_closed and lot.realized_pnl is not None:
                # Use closed_at date to determine period
                if lot.closed_at:
                    period_key, _ = _group_date_to_period_key(lot.closed_at, period_type)
                    if period_key not in period_data:
                        period_data[period_key] = {
                            "realized_pnl": ZERO,
                            "unrealized_exposure": ZERO,
                        }
                    period_data[period_key]["realized_pnl"] += lot.realized_pnl

    # Aggregate unrealized exposure from open lots
    # Unrealized exposure is the credit remaining on open positions
    # We attribute it to the period when each individual lot was opened
    for leg in matched_legs:
        for lot in leg.lots:
            if lot.is_open:
                # Attribute each lot's credit_remaining to the period when that lot was opened
                if lot.opened_at:
                    period_key, _ = _group_date_to_period_key(lot.opened_at, period_type)
                    if period_key not in period_data:
                        period_data[period_key] = {
                            "realized_pnl": ZERO,
                            "unrealized_exposure": ZERO,
                        }
                    period_data[period_key]["unrealized_exposure"] += lot.credit_remaining

    return period_data


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
    # Fetch transactions
    stored_txns = repository.fetch_transactions(
        account_name=account_name,
        account_number=account_number,
        ticker=ticker,
        since=since,
        until=until,
        status="all",
    )

    if not stored_txns:
        # Return empty report
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

    # Convert to normalized transactions
    normalized_txns = [_stored_to_normalized(stored) for stored in stored_txns]

    # Get matched legs for P&L calculation
    all_fills = group_fills_by_account(normalized_txns)
    matched_map, _errors = match_legs_with_errors(all_fills)
    matched_legs = list(matched_map.values())

    # Aggregate cash flow by period
    cash_flow_by_period = _aggregate_cash_flow_by_period(normalized_txns, period_type)

    # Aggregate P&L by period
    pnl_by_period = _aggregate_pnl_by_period(matched_legs, normalized_txns, period_type)

    # Combine into period metrics
    all_period_keys = set(cash_flow_by_period.keys()) | set(pnl_by_period.keys())
    periods: List[PeriodMetrics] = []

    for period_key in sorted(all_period_keys):
        # Get period label (use first transaction date for this period to generate label)
        period_label = period_key
        if period_key != "total":
            # Find a transaction in this period to generate the label
            for txn in normalized_txns:
                key, label = _group_date_to_period_key(txn.activity_date, period_type)
                if key == period_key:
                    period_label = label
                    break

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

    # Calculate totals
    total_credits = Decimal(sum(p.credits for p in periods))
    total_debits = Decimal(sum(p.debits for p in periods))
    total_realized_pnl = Decimal(sum(p.realized_pnl for p in periods))
    total_unrealized = Decimal(sum(p.unrealized_exposure for p in periods))

    totals = PeriodMetrics(
        period_key="total",
        period_label="Total",
        credits=total_credits,
        debits=total_debits,
        net_cash_flow=total_credits - total_debits,
        realized_pnl=total_realized_pnl,
        unrealized_exposure=total_unrealized,
        total_pnl=total_realized_pnl + total_unrealized,
    )

    return CashFlowPnlReport(
        account_name=account_name,
        account_number=account_number,
        period_type=period_type,
        periods=periods,
        totals=totals,
    )
