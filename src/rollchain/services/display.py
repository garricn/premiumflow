"""
Display formatting services for rollchain CLI.

This module provides formatting functions for displaying chains, transactions,
and other data in the CLI interface.
"""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

from ..services.options import OptionDescriptor, parse_option_description


def format_currency(value: Decimal | None) -> str:
    """Format a decimal value as currency."""
    if value is None:
        return "--"
    quantized = value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    sign = "-" if quantized < 0 else ""
    quantized = abs(quantized)
    return f"{sign}${quantized:,.2f}"


def format_breakeven(chain: Dict[str, Any]) -> str:
    """Format breakeven price with direction for display."""
    if chain.get("status") != "OPEN":
        return "--"
    breakeven = chain.get("breakeven_price")
    if breakeven is None:
        return "--"
    direction = chain.get("breakeven_direction") or ""
    return f"{format_currency(breakeven)} {direction}".strip()


def format_percent(value: Decimal) -> str:
    """Format a decimal value as a percentage."""
    percent = (value * Decimal('100')).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    text = f"{percent:,.2f}"
    if text.endswith(".00"):
        text = text[:-3]
    elif text.endswith("0"):
        text = text[:-1]
    return f"{text}%"


def format_price_range(value_pair: Optional[Tuple[Decimal, Decimal]]) -> str:
    """Format a price range tuple for display."""
    if not value_pair:
        return "--"
    low, high = value_pair
    return f"{format_currency(low)} - {format_currency(high)}"


def format_target_close_prices(price_list: Optional[List[Decimal]]) -> str:
    """Format a list of target close prices for display."""
    if not price_list:
        return "--"
    return ", ".join(format_currency(value) for value in price_list)


def ensure_display_name(chain: Dict[str, Any]) -> str:
    """Ensure a chain has a proper display name."""
    display = chain.get("display_name")
    if display:
        return display
    symbol = chain.get("symbol") or ""
    strike = chain.get("strike")
    option_label = chain.get("option_label") or ""
    if isinstance(strike, Decimal):
        if strike == strike.to_integral_value():
            strike_text = f"{int(strike)}"
        else:
            strike_text = f"{strike.quantize(Decimal('0.01')):,.2f}"
    else:
        strike_text = str(strike or "")
    
    parts = [symbol]
    if strike_text:
        parts.append(f"${strike_text}")
    if option_label:
        parts.append(option_label)
    
    result = " ".join(parts).strip()
    return result if result != symbol else symbol


def format_option_display(parsed: Optional[OptionDescriptor], fallback: str) -> Tuple[str, str]:
    """Format an option descriptor for display."""
    if not parsed:
        return fallback, ""
    strike_text = f"{parsed.strike.quantize(Decimal('0.01')):,.2f}"
    return f"{parsed.symbol} ${strike_text} {parsed.option_type}", parsed.expiration


def prepare_transactions_for_display(
    transactions: List[Dict[str, Any]],
    target_percents: List[Decimal],
) -> List[Dict[str, str]]:
    """Prepare transaction data for display formatting."""
    from ..services.targets import compute_target_close_prices
    
    rows: List[Dict[str, str]] = []
    for txn in transactions:
        parsed_option = parse_option_description(txn.get('Description', ''))
        formatted_desc, expiration = format_option_display(parsed_option, txn.get('Description', ''))
        target_prices = compute_target_close_prices(
            txn.get('Trans Code'),
            txn.get('Price'),
            target_percents,
        )

        rows.append(
            {
                "date": txn.get('Activity Date', ''),
                "symbol": (txn.get('Instrument') or '').strip(),
                "expiration": expiration,
                "code": txn.get('Trans Code', ''),
                "quantity": txn.get('Quantity', ''),
                "price": txn.get('Price', ''),
                "description": formatted_desc,
                "target_close": format_target_close_prices(target_prices),
            }
        )

    return rows


def prepare_chain_display(
    chain: Dict[str, Any], 
    target_bounds: Tuple[Decimal, Decimal]
) -> Dict[str, str]:
    """Prepare chain data for display formatting."""
    from .analysis import calculate_target_price_range
    
    return {
        "display_name": ensure_display_name(chain),
        "expiration": chain.get("expiration", "") or "N/A",
        "status": chain.get("status", "UNKNOWN"),
        "credits": format_currency(chain.get("total_credits")),
        "debits": format_currency(chain.get("total_debits")),
        "fees": format_currency(chain.get("total_fees")),
        "net_pnl": format_net_pnl(chain),
        "breakeven": format_breakeven(chain),
        "target_price": format_price_range(calculate_target_price_range(chain, target_bounds)),
    }


def format_net_pnl(chain: Dict[str, Any]) -> str:
    """Format net P&L for display."""
    if chain.get("status") != "CLOSED":
        return format_realized_pnl(chain)
    return format_currency(chain.get("net_pnl_after_fees"))


def format_realized_pnl(chain: Dict[str, Any]) -> str:
    """Format realized P&L for display."""
    from .analysis import calculate_realized_pnl
    return format_currency(calculate_realized_pnl(chain))
