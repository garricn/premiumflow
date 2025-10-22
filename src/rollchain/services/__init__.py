"""Services for roll chain analysis."""

from .chain_builder import detect_roll_chains
from .analyzer import calculate_pnl, calculate_breakeven
from .display import (
    format_currency,
    format_breakeven,
    format_percent,
    format_price_range,
    format_target_close_prices,
    ensure_display_name,
    format_option_display,
    prepare_transactions_for_display,
    prepare_chain_display,
    format_net_pnl,
    format_realized_pnl,
    calculate_target_price_range,
)

__all__ = [
    "detect_roll_chains", 
    "calculate_pnl", 
    "calculate_breakeven",
    "format_currency",
    "format_breakeven",
    "format_percent",
    "format_price_range",
    "format_target_close_prices",
    "ensure_display_name",
    "format_option_display",
    "prepare_transactions_for_display",
    "prepare_chain_display",
    "format_net_pnl",
    "format_realized_pnl",
    "calculate_target_price_range",
]
