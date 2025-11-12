from __future__ import annotations

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


def _collect_pnl_period_keys(  # noqa: PLR0913
    matched_legs: List[MatchedLeg],
    transactions: List[NormalizedOptionTransaction],
    period_type: PeriodType,
    *,
    since: Optional[date] = None,
    until: Optional[date] = None,
    clamp_periods_to_range: bool = True,
) -> Set[str]:
    """
    Collect the set of period keys that matter for P&L aggregation.
    """
    all_period_keys: Set[str] = set()

    for txn in transactions:
        period_key, _ = _group_date_to_period_key(txn.activity_date, period_type)
        all_period_keys.add(period_key)

    for leg in matched_legs:
        for lot in leg.lots:
            if lot.is_closed and lot.realized_pnl is not None:
                if lot.closed_at and _date_in_range(lot.closed_at, since, until):
                    period_key, _ = _group_date_to_period_key(lot.closed_at, period_type)
                    all_period_keys.add(period_key)
            if _lot_was_open_during_period(lot, until):
                if lot.opened_at and _lot_overlaps_date_range(lot.opened_at, until):
                    period_key, _ = _group_date_to_period_key(lot.opened_at, period_type)
                    if clamp_periods_to_range:
                        period_key = _clamp_period_to_range(period_key, period_type, since)
                    all_period_keys.add(period_key)

    return all_period_keys
