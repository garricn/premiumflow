from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import List, Optional, Set

from ..core.parser import NormalizedOptionTransaction
from .cash_flow_periods import (
    PeriodType,
    _clamp_period_to_range,
    _date_in_range,
    _group_date_to_period_key,
    _lot_overlaps_date_range,
    _lot_was_open_during_period,
)
from .leg_matching import MatchedLeg


@dataclass
class PnlPeriodCollectionOptions:
    """Options for collecting P&L period keys."""

    since: Optional[date] = None
    until: Optional[date] = None
    clamp_periods_to_range: bool = True


def _collect_pnl_period_keys(
    matched_legs: List[MatchedLeg],
    transactions: List[NormalizedOptionTransaction],
    period_type: PeriodType,
    *,
    options: Optional[PnlPeriodCollectionOptions] = None,
) -> Set[str]:
    """
    Collect the universe of period keys required for P&L aggregation upfront.
    """
    if options is None:
        options = PnlPeriodCollectionOptions()

    all_period_keys: Set[str] = set()

    for txn in transactions:
        period_key, _ = _group_date_to_period_key(txn.activity_date, period_type)
        all_period_keys.add(period_key)

    for leg in matched_legs:
        for lot in leg.lots:
            if lot.is_closed and lot.realized_pnl is not None:
                if lot.closed_at and _date_in_range(lot.closed_at, options.since, options.until):
                    period_key, _ = _group_date_to_period_key(lot.closed_at, period_type)
                    all_period_keys.add(period_key)
            if _lot_was_open_during_period(lot, options.until):
                if lot.opened_at and _lot_overlaps_date_range(lot.opened_at, options.until):
                    period_key, _ = _group_date_to_period_key(lot.opened_at, period_type)
                    if options.clamp_periods_to_range:
                        period_key = _clamp_period_to_range(period_key, period_type, options.since)
                    all_period_keys.add(period_key)

    return all_period_keys
