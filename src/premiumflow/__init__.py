"""
RollChain - Options trading roll chain analysis tool.

A Python package for analyzing options trading roll chains from transaction data.
"""

__version__ = "0.1.0"
__author__ = "Garric Nahapetian"
__email__ = "garricn@users.noreply.github.com"

# Import main components for easy access
from .core.models import Transaction, RollChain
from .core.parser import (
    parse_csv_file,
    is_options_transaction,
    is_call_option,
    is_put_option,
    format_position_spec,
    parse_lookup_input,
)
from .services.chain_builder import detect_roll_chains
from .services.analyzer import calculate_pnl, calculate_breakeven
from .formatters.output import format_roll_chain_summary


# Legacy function compatibility - need to implement find_chain_by_position
def find_chain_by_position(position_spec, chains):
    """Find a chain by position specification (legacy compatibility)."""
    for chain in chains:
        if chain.get("symbol") and chain.get("strike"):
            chain_spec = f"{chain['symbol']} ${chain['strike']} {chain.get('option_type', 'C')}"
            if position_spec.lower() in chain_spec.lower():
                return chain
    return None


__all__ = [
    "Transaction",
    "RollChain",
    "parse_csv_file",
    "detect_roll_chains",
    "calculate_pnl",
    "calculate_breakeven",
    "format_roll_chain_summary",
    "is_options_transaction",
    "is_call_option",
    "is_put_option",
    "format_position_spec",
    "parse_lookup_input",
    "find_chain_by_position",
]
