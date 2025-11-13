from __future__ import annotations

from decimal import Decimal
from typing import Dict, List

from ..core.parser import NormalizedOptionTransaction
from .cash_flow_aggregations import ZERO
from .cash_flow_models import (
    PeriodMetrics,
    RealizedView,
    RealizedViewTotals,
    _build_realized_breakdowns,
    _OptionsRealizedTotals,
    _StockRealizedTotals,
)
from .cash_flow_periods import (
    PeriodType,
    _group_date_to_period_key,
    _parse_period_key_to_date,
)
from .cash_flow_pnl_aggregators import _empty_period_entry


def _generate_period_label(
    period_key: str,
    transactions: List["NormalizedOptionTransaction"],
    period_type: PeriodType,
) -> str:
    """Generate a human-readable label for a period key."""
    if period_key == "total":
        return "Total"

    for txn in transactions:
        key, label = _group_date_to_period_key(txn.activity_date, period_type)
        if key == period_key:
            return label

    period_date = _parse_period_key_to_date(period_key, period_type)
    if period_date:
        _, label = _group_date_to_period_key(period_date, period_type)
        return label
    return period_key


def _empty_stock_realized_entry() -> Dict[str, Decimal]:
    """Return a zeroed-out holder for stock realized P&L components."""
    return {"profits": ZERO, "losses": ZERO, "net": ZERO}


def _sum_realized_breakdown(periods: List[PeriodMetrics], view: RealizedView) -> RealizedViewTotals:
    """Sum realized breakdowns across all periods for the given view."""
    decimal_zero = Decimal(0)
    return RealizedViewTotals(
        profits_gross=sum(
            (Decimal(p.realized_breakdowns[view].profits_gross) for p in periods), decimal_zero
        ),
        losses_gross=sum(
            (Decimal(p.realized_breakdowns[view].losses_gross) for p in periods), decimal_zero
        ),
        net_gross=sum(
            (Decimal(p.realized_breakdowns[view].net_gross) for p in periods), decimal_zero
        ),
        profits_net=sum(
            (Decimal(p.realized_breakdowns[view].profits_net) for p in periods), decimal_zero
        ),
        losses_net=sum(
            (Decimal(p.realized_breakdowns[view].losses_net) for p in periods), decimal_zero
        ),
        net_net=sum((Decimal(p.realized_breakdowns[view].net_net) for p in periods), decimal_zero),
    )


def _build_period_metrics(
    cash_flow_by_period: Dict[str, Dict[str, Decimal]],
    pnl_by_period: Dict[str, Dict[str, Decimal]],
    stock_realized_by_period: Dict[str, Dict[str, Decimal]],
    filtered_transactions: List["NormalizedOptionTransaction"],
    period_type: PeriodType,
) -> List[PeriodMetrics]:
    """Combine cash flow, P&L, and stock data into `PeriodMetrics` objects."""
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
            options=_OptionsRealizedTotals(
                profits_gross=realized_profits_gross,
                losses_gross=realized_losses_gross,
                pnl_gross=realized_pnl_gross,
                profits_net=realized_profits_net,
                losses_net=realized_losses_net,
                pnl_net=realized_pnl_net,
            ),
            stock=_StockRealizedTotals(
                profits=stock_profits,
                losses=stock_losses,
                net=stock_net,
            ),
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
    """Calculate grand totals across periods."""
    decimal_zero = Decimal(0)
    total_credits = sum((Decimal(p.credits) for p in periods), decimal_zero)
    total_debits = sum((Decimal(p.debits) for p in periods), decimal_zero)
    total_realized_profits_gross = sum(
        (Decimal(p.realized_profits_gross) for p in periods), decimal_zero
    )
    total_realized_losses_gross = sum(
        (Decimal(p.realized_losses_gross) for p in periods), decimal_zero
    )
    total_realized_pnl_gross = sum((Decimal(p.realized_pnl_gross) for p in periods), decimal_zero)
    total_realized_profits_net = sum(
        (Decimal(p.realized_profits_net) for p in periods), decimal_zero
    )
    total_realized_losses_net = sum((Decimal(p.realized_losses_net) for p in periods), decimal_zero)
    total_realized_pnl_net = sum((Decimal(p.realized_pnl_net) for p in periods), decimal_zero)
    total_assignment_realized_gross = sum(
        (Decimal(p.assignment_realized_gross) for p in periods), decimal_zero
    )
    total_assignment_realized_net = sum(
        (Decimal(p.assignment_realized_net) for p in periods), decimal_zero
    )
    total_unrealized_exposure = sum((Decimal(p.unrealized_exposure) for p in periods), decimal_zero)
    total_opening_fees = sum((Decimal(p.opening_fees) for p in periods), decimal_zero)
    total_closing_fees = sum((Decimal(p.closing_fees) for p in periods), decimal_zero)
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


__all__ = ["_build_period_metrics", "_calculate_totals"]
