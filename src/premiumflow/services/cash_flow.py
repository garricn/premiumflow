# file-length-ignore
"""
Account-level cash flow and P&L reporting service.

This module provides functions to aggregate cash flow and profit/loss metrics
across all imports for an account, with time-based grouping (daily, weekly,
monthly, total).
"""

from __future__ import annotations

from datetime import date
from typing import Optional

from ..persistence import SQLiteRepository
from .cash_flow_helpers import PeriodType, _group_date_to_period_key
from .cash_flow_models import (
    AssignmentHandling,
    CashFlowPnlReport,
    PeriodMetrics,
    RealizedView,
    RealizedViewTotals,
)

__all__ = [
    "AssignmentHandling",
    "CashFlowPnlReport",
    "PeriodMetrics",
    "RealizedView",
    "RealizedViewTotals",
    "_group_date_to_period_key",
]


def generate_cash_flow_pnl_report(
    repository: SQLiteRepository,
    account_name: str,
    account_number: Optional[str] = None,
    period_type: PeriodType = "total",
    **kwargs: object,
) -> CashFlowPnlReport:
    """
    Generate account-level cash flow and P&L report.

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
            periods to the first period in the range (default: True)
        assignment_handling (str): "include" or "exclude" assignment-derived
            realized P&L from totals (default: "include")

    Returns
    -------
    CashFlowPnlReport
        Complete report with period-based metrics and totals
    """
    from .cash_flow_report import generate_cash_flow_pnl_report as _impl

    ticker: Optional[str] = kwargs.get("ticker")  # type: ignore[assignment]
    since: Optional[date] = kwargs.get("since")  # type: ignore[assignment]
    until: Optional[date] = kwargs.get("until")  # type: ignore[assignment]
    clamp_periods_to_range: bool = kwargs.get(
        "clamp_periods_to_range", True  # type: ignore[assignment]
    )
    assignment_handling: AssignmentHandling = kwargs.get(
        "assignment_handling", "include"  # type: ignore[assignment]
    )

    return _impl(
        repository=repository,
        account_name=account_name,
        account_number=account_number,
        period_type=period_type,
        ticker=ticker,
        since=since,
        until=until,
        clamp_periods_to_range=clamp_periods_to_range,
        assignment_handling=assignment_handling,
    )
