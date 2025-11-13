from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Dict, List, Literal, Optional

from .cash_flow_aggregations import ZERO
from .cash_flow_periods import PeriodType

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


@dataclass
class _CashFlowPnlReportParams:
    """Bundle all filtering and behavioral options for cash flow P&L reports."""

    ticker: Optional[str] = None
    since: Optional[date] = None
    until: Optional[date] = None
    clamp_periods_to_range: bool = True
    assignment_handling: AssignmentHandling = "include"


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


__all__ = [
    "AssignmentHandling",
    "RealizedView",
    "PeriodMetrics",
    "CashFlowPnlReport",
    "RealizedViewTotals",
    "_CashFlowPnlReportParams",
    "_OptionsRealizedTotals",
    "_StockRealizedTotals",
    "_zero_realized_view_totals",
    "_empty_realized_breakdowns",
    "_build_realized_breakdowns",
    "_sum_realized_breakdown",
]
