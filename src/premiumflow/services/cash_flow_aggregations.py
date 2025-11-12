from __future__ import annotations

from decimal import Decimal
from typing import Dict, List

from ..core.parser import NormalizedOptionTransaction
from .cash_flow_periods import PeriodType, _group_date_to_period_key

CONTRACT_MULTIPLIER = Decimal("100")
ZERO = Decimal("0")


def _calculate_cash_value(txn: NormalizedOptionTransaction) -> Decimal:
    """
    Determine the cash movement for a normalized option transaction.

    Prefer the broker-supplied ``Amount`` when available; otherwise, derive parity
    from ``price * quantity * 100`` taking into account the trade direction.
    """
    if txn.amount is not None:
        return txn.amount

    base_value = txn.price * Decimal(txn.quantity) * CONTRACT_MULTIPLIER
    return base_value if txn.action == "SELL" else -base_value


def _aggregate_cash_flow_by_period(
    transactions: List[NormalizedOptionTransaction],
    period_type: PeriodType,
) -> Dict[str, Dict[str, Decimal]]:
    """
    Aggregate cash flow (credits/debits) grouped by the requested period type.
    """
    period_data: Dict[str, Dict[str, Decimal]] = {}

    for txn in transactions:
        cash_value = _calculate_cash_value(txn)
        period_key, _ = _group_date_to_period_key(txn.activity_date, period_type)

        entry = period_data.setdefault(period_key, {"credits": ZERO, "debits": ZERO})
        if cash_value >= ZERO:
            entry["credits"] += cash_value
        else:
            entry["debits"] += -cash_value

    return period_data
