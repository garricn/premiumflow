"""
P&L analysis and calculations for roll chains.

This module handles profit/loss calculations and analysis.
"""

from decimal import Decimal
from typing import Any, Dict, List, Optional

from ..core.models import RollChain, Transaction


def calculate_credits(transactions: List[Transaction]) -> Decimal:
    """Calculate total credits from sell transactions."""
    return sum(
        (t.price * abs(t.quantity) for t in transactions if t.action == "SELL"),
        Decimal("0"),
    )


def calculate_debits(transactions: List[Transaction]) -> Decimal:
    """Calculate total debits from buy transactions."""
    return sum(
        (t.price * abs(t.quantity) for t in transactions if t.action == "BUY"),
        Decimal("0"),
    )


def calculate_pnl(transactions: List[Transaction]) -> Decimal:
    """Calculate net profit/loss for a list of transactions."""
    credits = calculate_credits(transactions)
    debits = calculate_debits(transactions)
    return credits - debits


def calculate_breakeven(transactions: List[Transaction], strike: Decimal) -> Optional[Decimal]:
    """Calculate breakeven price for a position."""
    net_quantity = sum(t.net_quantity for t in transactions)

    if net_quantity == 0:
        return None

    pnl = calculate_pnl(transactions)
    return strike + (pnl / abs(net_quantity))


def analyze_roll_chain(chain: RollChain) -> Dict[str, Any]:
    """Perform comprehensive analysis of a roll chain."""
    return {
        "symbol": chain.symbol,
        "strike": chain.strike,
        "option_type": chain.option_type,
        "expiration": chain.expiration,
        "net_quantity": chain.net_quantity,
        "total_credits": chain.total_credits,
        "total_debits": chain.total_debits,
        "net_pnl": chain.net_pnl,
        "breakeven_price": chain.breakeven_price,
        "is_closed": chain.is_closed,
        "is_open": chain.is_open,
        "transaction_count": len(chain.transactions),
    }
