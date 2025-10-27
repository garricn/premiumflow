"""
Chain analysis services for premiumflow.

This module provides business logic for analyzing roll chains,
calculating P&L, and determining chain status.
"""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Dict, List, Optional, Tuple


def is_open_chain(chain: Dict[str, Any]) -> bool:
    """Determine whether a detected chain is still open."""
    status = (chain.get("status") or "").upper()
    if status in {"OPEN", "CLOSED"}:
        return status == "OPEN"

    transactions: List[Dict[str, Any]] = chain.get("transactions") or []
    if not transactions:
        return False
    last_code = (transactions[-1].get("Trans Code") or "").strip().upper()
    return last_code in {"STO", "BTO"}


def calculate_realized_pnl(chain: Dict[str, Any]) -> Decimal:
    """Calculate realized P&L for a chain."""
    total_credits = chain.get("total_credits") or Decimal("0")
    total_debits = chain.get("total_debits") or Decimal("0")
    total_fees = chain.get("total_fees") or Decimal("0")
    return total_credits - total_debits - total_fees


def calculate_target_price_range(
    chain: Dict[str, Any], bounds: Tuple[Decimal, Decimal]
) -> Optional[Tuple[Decimal, Decimal]]:
    """Calculate target price range for a chain."""
    breakeven = chain.get("breakeven_price")
    net_contracts = chain.get("net_contracts", 0)
    if breakeven is None or not net_contracts:
        return None

    realized = calculate_realized_pnl(chain)
    contracts = abs(net_contracts)
    if contracts == 0:
        return None

    per_share_realized = realized / (Decimal(contracts) * Decimal("100"))
    per_share_realized = per_share_realized.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    if per_share_realized <= Decimal("0"):
        return None

    lower_shift = (per_share_realized * bounds[0]).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    upper_shift = (per_share_realized * bounds[1]).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    breakeven = Decimal(breakeven)
    if net_contracts < 0:
        low_price = breakeven - upper_shift
        high_price = breakeven - lower_shift
    else:
        low_price = breakeven + lower_shift
        high_price = breakeven + upper_shift

    return low_price, high_price


def filter_open_chains(chains: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filter chains to only include open positions."""
    return [chain for chain in chains if is_open_chain(chain)]
