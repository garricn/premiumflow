"""Shared helpers for loading normalized transactions and matched legs."""

from __future__ import annotations

from typing import List, Optional

from ..core.parser import NormalizedOptionTransaction
from ..persistence import SQLiteRepository
from .leg_matching import (
    MatchedLeg,
    _stored_to_normalized,
    group_fills_by_account,
    match_legs_with_errors,
)


def fetch_normalized_transactions(
    repository: SQLiteRepository,
    *,
    account_name: str,
    account_number: Optional[str] = None,
    ticker: Optional[str] = None,
) -> List[NormalizedOptionTransaction]:
    """Fetch stored transactions for an account and normalize them."""

    stored_txns = repository.fetch_transactions(
        account_name=account_name,
        account_number=account_number,
        ticker=ticker,
        status="all",
    )
    return [_stored_to_normalized(stored) for stored in stored_txns]


def match_legs_from_transactions(
    transactions: List[NormalizedOptionTransaction],
) -> List[MatchedLeg]:
    """Match legs from normalized transactions."""

    if not transactions:
        return []

    fills = group_fills_by_account(transactions)
    matched_map, _errors = match_legs_with_errors(fills)
    return list(matched_map.values())
