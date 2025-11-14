from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Dict, List, Optional

from ..core.parser import NormalizedOptionTransaction
from ..persistence import SQLiteRepository
from .cash_flow_helpers import (
    ZERO,
    PeriodType,
    _aggregate_cash_flow_by_period,
    _aggregate_pnl_by_period,
    _calculate_totals,
    _date_in_range,
    _group_date_to_period_key,
    _PnlAggregationOptions,
)
from .cash_flow_models import (
    AssignmentHandling,
    CashFlowPnlReport,
    PeriodMetrics,
    _CashFlowPnlReportParams,
    _empty_realized_breakdowns,
)
from .cash_flow_period_metrics import _build_period_metrics, _empty_stock_realized_entry
from .stock_lots import StockLotSummary, fetch_stock_lot_summaries
from .transaction_loader import fetch_normalized_transactions, match_legs_from_transactions


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


def _generate_cash_flow_pnl_report_impl(
    repository: SQLiteRepository,
    account_name: str,
    account_number: Optional[str],
    period_type: PeriodType,
    params: _CashFlowPnlReportParams,
) -> CashFlowPnlReport:
    """
    Generate account-level cash flow and P&L report with time-based grouping.
    """
    all_normalized_txns = fetch_normalized_transactions(
        repository,
        account_name=account_name,
        account_number=account_number,
        ticker=params.ticker,
    )

    if not all_normalized_txns:
        return _create_empty_report(account_name, account_number, period_type)

    matched_legs = match_legs_from_transactions(all_normalized_txns)
    filtered_txns = _filter_transactions_by_date(
        all_normalized_txns,
        since=params.since,
        until=params.until,
    )

    stock_lot_summaries = fetch_stock_lot_summaries(
        repository,
        account_name=account_name,
        account_number=account_number,
        ticker=params.ticker,
        status="closed",
    )
    stock_realized_by_period = _aggregate_stock_realized_by_period(
        stock_lot_summaries,
        period_type,
        since=params.since,
        until=params.until,
    )

    cash_flow_by_period = _aggregate_cash_flow_by_period(filtered_txns, period_type)
    pnl_options = _PnlAggregationOptions(
        since=params.since,
        until=params.until,
        clamp_periods_to_range=params.clamp_periods_to_range,
        assignment_handling=params.assignment_handling,
    )
    pnl_by_period = _aggregate_pnl_by_period(
        matched_legs,
        filtered_txns,
        period_type,
        pnl_options,
    )

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


def generate_cash_flow_pnl_report(
    repository: SQLiteRepository,
    account_name: str,
    account_number: Optional[str] = None,
    period_type: PeriodType = "total",
    **kwargs: object,
) -> CashFlowPnlReport:
    """
    Generate account-level cash flow and P&L report with period-based grouping.

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
    **kwargs
        Optional filtering and behavioral options:
        ticker (str): ticker symbol to filter by
        since (date): start date for filtering
        until (date): end date for filtering
        clamp_periods_to_range (bool): whether to clamp unrealized exposure
            periods (default: True)
        assignment_handling (str): "include" or "exclude" assignment-derived
            P&L (default: "include")
    """
    ticker: Optional[str] = kwargs.get("ticker")  # type: ignore[assignment]
    since: Optional[date] = kwargs.get("since")  # type: ignore[assignment]
    until: Optional[date] = kwargs.get("until")  # type: ignore[assignment]
    clamp_periods_to_range: bool = kwargs.get(
        "clamp_periods_to_range", True  # type: ignore[assignment]
    )
    assignment_handling: AssignmentHandling = kwargs.get(
        "assignment_handling", "include"  # type: ignore[assignment]
    )

    params = _CashFlowPnlReportParams(
        ticker=ticker,
        since=since,
        until=until,
        clamp_periods_to_range=clamp_periods_to_range,
        assignment_handling=assignment_handling,
    )
    return _generate_cash_flow_pnl_report_impl(
        repository=repository,
        account_name=account_name,
        account_number=account_number,
        period_type=period_type,
        params=params,
    )


__all__ = ["generate_cash_flow_pnl_report"]
