"""Transaction filtering helpers."""

from typing import Any, Dict, Iterable, List, Optional, Tuple


def filter_transactions_by_ticker(
    transactions: Iterable[Dict[str, Any]],
    ticker_symbol: Optional[str],
) -> List[Dict[str, Any]]:
    if not ticker_symbol:
        return list(transactions)

    ticker_key = ticker_symbol.strip().upper()
    return [
        txn
        for txn in transactions
        if (txn.get('Instrument') or '').strip().upper() == ticker_key
    ]


def filter_transactions_by_option_type(
    transactions: Iterable[Dict[str, Any]],
    *,
    calls_only: bool = False,
    puts_only: bool = False,
) -> List[Dict[str, Any]]:
    if calls_only and puts_only:
        raise ValueError("Cannot combine --calls-only and --puts-only")

    transactions = list(transactions)
    if calls_only:
        return [
            txn for txn in transactions
            if 'call' in (txn.get('Description') or '').lower()
        ]
    if puts_only:
        return [
            txn for txn in transactions
            if 'put' in (txn.get('Description') or '').lower()
        ]
    return transactions


def filter_open_positions(transactions: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filter transactions to show only positions that are still open (net quantity > 0)."""
    transactions = list(transactions)
    closing_codes = {"STC", "BTC"}
    opening_codes = {"STO", "BTO"}

    def _txn_key(txn: Dict[str, Any]) -> Tuple[str, str]:
        return (
            (txn.get('Instrument') or '').strip().upper(),
            (txn.get('Description') or '').strip(),
        )

    def _parse_quantity(quantity_str: str) -> int:
        """Parse quantity string, handling negative values in parentheses."""
        if not quantity_str:
            return 0
        cleaned = quantity_str.replace(',', '').strip()
        if cleaned.startswith('(') and cleaned.endswith(')'):
            cleaned = f"-{cleaned[1:-1]}"
        try:
            return int(float(cleaned))
        except (ValueError, TypeError):
            return 0

    # Group transactions by position (instrument + description)
    position_quantities = {}
    
    for txn in transactions:
        trans_code = (txn.get('Trans Code') or '').strip().upper()
        if trans_code not in opening_codes and trans_code not in closing_codes:
            continue
            
        key = _txn_key(txn)
        quantity = _parse_quantity(txn.get('Quantity', '0'))
        
        if key not in position_quantities:
            position_quantities[key] = 0
            
        if trans_code in opening_codes:
            # Opening transactions increase position quantity
            position_quantities[key] += quantity
        elif trans_code in closing_codes:
            # Closing transactions decrease position quantity
            position_quantities[key] -= quantity

    # Return opening transactions for positions that are still open (net quantity > 0)
    open_positions = []
    for txn in transactions:
        trans_code = (txn.get('Trans Code') or '').strip().upper()
        if trans_code in opening_codes:
            key = _txn_key(txn)
            if position_quantities.get(key, 0) > 0:
                open_positions.append(txn)
    
    return open_positions
