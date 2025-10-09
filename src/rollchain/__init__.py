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
    get_options_transactions,
)
from .services.chain_builder import detect_roll_chains
from .services.analyzer import calculate_pnl, calculate_breakeven
from .formatters.output import format_roll_chain_summary

# Legacy function compatibility
def find_chain_by_position(csv_file, lookup_spec):
    """Find a roll chain that contains the specified position."""
    # Support both dicts (new callers) and tuples (legacy parse_lookup_input output)
    if isinstance(lookup_spec, (list, tuple)) and len(lookup_spec) == 4:
        ticker, strike, option_type, expiration = lookup_spec
        lookup = {
            'ticker': str(ticker).strip().upper(),
            'strike': str(strike).replace('$', ''),
            'option_type': str(option_type).strip().upper(),
            'expiration': str(expiration).strip(),
        }
    elif isinstance(lookup_spec, dict):
        lookup = {
            'ticker': str(lookup_spec.get('ticker', '')).strip().upper(),
            'strike': str(lookup_spec.get('strike', '')).replace('$', ''),
            'option_type': str(lookup_spec.get('option_type', '')).strip().upper(),
            'expiration': str(lookup_spec.get('expiration', '')).strip(),
        }
    else:
        raise TypeError("lookup_spec must be a dict or a 4-tuple of lookup values")

    if not lookup['ticker'] or not lookup['strike'] or not lookup['option_type']:
        raise ValueError("lookup_spec must include ticker, strike, and option_type")

    options_txns = get_options_transactions(csv_file)

    calls_by_ticker = {}
    puts_by_ticker = {}

    for txn in options_txns:
        instrument = (txn.get('Instrument') or '').strip().upper()
        description = (txn.get('Description') or '').strip()

        if not instrument or not description:
            continue

        if 'CALL' in description.upper():
            calls_by_ticker.setdefault(instrument, []).append(txn)
        elif 'PUT' in description.upper():
            puts_by_ticker.setdefault(instrument, []).append(txn)

    option_type = lookup['option_type']
    ticker = lookup['ticker']

    if option_type in ('CALL', 'C'):
        ticker_txns = calls_by_ticker.get(ticker, [])
    elif option_type in ('PUT', 'P'):
        ticker_txns = puts_by_ticker.get(ticker, [])
    else:
        ticker_txns = calls_by_ticker.get(ticker, []) + puts_by_ticker.get(ticker, [])

    if not ticker_txns:
        return None

    chains = detect_roll_chains(ticker_txns)

    strike_fragment = f"${lookup['strike']}"
    option_fragment = 'Call' if option_type in ('CALL', 'C') else 'Put'
    expiration_fragment = lookup['expiration']

    for chain in chains:
        for txn in chain.get('transactions', []):
            description = txn.get('Description', '')

            if (
                ticker in description
                and strike_fragment in description
                and option_fragment in description
                and (not expiration_fragment or expiration_fragment in description)
            ):
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