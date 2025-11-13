from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Dict, List, Literal, Optional

from ..core.parser import NormalizedOptionTransaction
from .cash_flow_aggregations import ZERO
from .cash_flow_periods import (
    PeriodType,
    _clamp_period_to_range,
    _date_in_range,
    _group_date_to_period_key,
    _lot_closed_by_assignment,
    _lot_overlaps_date_range,
    _lot_was_open_during_period,
)
from .cash_flow_pnl_keys import PnlPeriodCollectionOptions, _collect_pnl_period_keys
from .leg_matching import MatchedLeg


@dataclass
class _UnrealizedExposureOptions:
    """Bundle optional parameters for unrealized exposure aggregation."""

    since: Optional[date] = None
    until: Optional[date] = None
    clamp_periods_to_range: bool = True


@dataclass
class _PnlAggregationOptions:
    """Bundle optional parameters for P&L period aggregation."""

    since: Optional[date] = None
    until: Optional[date] = None
    clamp_periods_to_range: bool = True
    assignment_handling: Literal["include", "exclude"] = "include"


@dataclass
class _RealizedPnlOptions:
    """Bundle optional parameters for realized P&L aggregation."""

    since: Optional[date] = None
    until: Optional[date] = None
    assignment_handling: Literal["include", "exclude"] = "include"


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


def _aggregate_realized_pnl(  # noqa: C901
    matched_legs: List[MatchedLeg],
    period_type: PeriodType,
    period_data: Dict[str, Dict[str, Decimal]],
    options: Optional[_RealizedPnlOptions] = None,
) -> None:
    if options is None:
        options = _RealizedPnlOptions()
    for leg in matched_legs:
        for lot in leg.lots:
            if lot.is_closed and lot.realized_pnl is not None and lot.closed_at:
                if not _date_in_range(lot.closed_at, options.since, options.until):
                    continue

                period_key, _ = _group_date_to_period_key(lot.closed_at, period_type)

                is_assignment_close = _lot_closed_by_assignment(lot)
                include_assignment = not (
                    is_assignment_close and options.assignment_handling == "exclude"
                )

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


def _aggregate_opening_fees(  # noqa: C901
    matched_legs: List[MatchedLeg],
    period_type: PeriodType,
    period_data: Dict[str, Dict[str, Decimal]],
    options: Optional[_UnrealizedExposureOptions] = None,
) -> None:
    if options is None:
        options = _UnrealizedExposureOptions()
    for leg in matched_legs:
        for lot in leg.lots:
            if lot.opened_at is None:
                continue
            if options.since is not None and lot.closed_at and lot.closed_at < options.since:
                continue

            period_key, _ = _group_date_to_period_key(lot.opened_at, period_type)
            if options.since is not None and lot.opened_at < options.since:
                if not options.clamp_periods_to_range:
                    continue
                period_key = _clamp_period_to_range(period_key, period_type, options.since)
            elif not _date_in_range(lot.opened_at, options.since, options.until):
                continue

            open_fees = Decimal(lot.open_fees)
            if open_fees == ZERO:
                continue
            if period_key not in period_data:
                if not options.clamp_periods_to_range:
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
    options: Optional[_UnrealizedExposureOptions] = None,
) -> None:
    if options is None:
        options = _UnrealizedExposureOptions()
    for leg in matched_legs:
        for lot in leg.lots:
            if _lot_was_open_during_period(lot, options.until):
                if lot.opened_at and _lot_overlaps_date_range(lot.opened_at, options.until):
                    period_key, _ = _group_date_to_period_key(lot.opened_at, period_type)
                    if options.clamp_periods_to_range:
                        period_key = _clamp_period_to_range(period_key, period_type, options.since)
                    exposure = lot.credit_remaining if lot.is_open else lot.open_premium
                    period_data[period_key]["unrealized_exposure"] += exposure


def _aggregate_pnl_by_period(
    matched_legs: List[MatchedLeg],
    transactions: List[NormalizedOptionTransaction],
    period_type: PeriodType,
    options: Optional[_PnlAggregationOptions] = None,
) -> Dict[str, Dict[str, Decimal]]:
    if options is None:
        options = _PnlAggregationOptions()

    collection_options = PnlPeriodCollectionOptions(
        since=options.since,
        until=options.until,
        clamp_periods_to_range=options.clamp_periods_to_range,
    )
    all_period_keys = _collect_pnl_period_keys(
        matched_legs,
        transactions,
        period_type,
        options=collection_options,
    )

    period_data: Dict[str, Dict[str, Decimal]] = {}
    for period_key in all_period_keys:
        period_data[period_key] = _empty_period_entry()

    realized_pnl_options = _RealizedPnlOptions(
        since=options.since,
        until=options.until,
        assignment_handling=options.assignment_handling,
    )
    _aggregate_realized_pnl(
        matched_legs,
        period_type,
        period_data,
        realized_pnl_options,
    )

    opening_fees_options = _UnrealizedExposureOptions(
        since=options.since,
        until=options.until,
        clamp_periods_to_range=options.clamp_periods_to_range,
    )
    _aggregate_opening_fees(
        matched_legs,
        period_type,
        period_data,
        opening_fees_options,
    )
    _aggregate_closing_fees(
        matched_legs, period_type, period_data, since=options.since, until=options.until
    )
    exposure_options = _UnrealizedExposureOptions(
        since=options.since,
        until=options.until,
        clamp_periods_to_range=options.clamp_periods_to_range,
    )
    _aggregate_unrealized_exposure(
        matched_legs,
        period_type,
        period_data,
        exposure_options,
    )

    return period_data


__all__ = [
    "_aggregate_pnl_by_period",
    "_empty_period_entry",
]
